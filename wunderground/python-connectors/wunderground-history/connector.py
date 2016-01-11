from dataiku.connector import Connector
import os
import requests
import json
import base64
import datetime
from time import sleep


class MyConnector(Connector):

    def __init__(self, config):
        Connector.__init__(self, config)

        self.api_key = self.config.get("api_key")
        self.location = self.config.get("location")
        self.from_date = self.config.get("from_date")
        self.to_date = self.config.get("to_date")
        self.cache_folder = self.config.get("cache_folder")
        filename = "cache-wunderground-history-%s.json" % base64.urlsafe_b64encode(self.location)
        self.cache_file = os.path.join(self.cache_folder, filename)

        # Cache directory
        if not os.path.isdir(self.cache_folder):
            os.makedirs(self.cache_folder)

        # Create cache file if does not exist
        if not os.path.exists(self.cache_file):
            with open(self.cache_file, 'w') as f:
                json.dump({}, f)
                f.close()


    def get_read_schema(self):

        return None

        # return {
        #         "columns" : [
        #             { "name" : "json", "type" : "json" }
        #         ]
        #     }


    def get_weather(self, day=None):

        if not day:
            return None

        # Reading json cache
        with open(self.cache_file, 'r') as f:
            cache = json.load(f)
            f.close()

        day_key = day.strftime("%d/%m/%Y")

        if day_key in cache.keys():

            # In cache
            print  "Wunderground plugin - Already in cache for %s" % day_key
            return cache.get(day_key)

        else:

            # Not in cache -> API request
            print  "Wunderground plugin - Request for %s" % day_key

            r = requests.get('https://api.wunderground.com/api/' + self.api_key + '/history_' + day.strftime("%Y%m%d") + '/q/' + self.location + '.json')
            
            # verification of the status code
            if r.status_code != 200:
                print "Wunderground plugin - Error in request (status code: %s)" % r.status_code
                r.raise_for_status()
                sys.exit()

            result = r.json()
            print "Wunderground plugin - API response: %s" % json.dumps(result)

            # verification of the json response
            if 'response' in result and 'error' in result.get('response'):
                raise IOError("API error for %s: %s" % (day_key, result.get('response').get('error').get('description')) )
            if 'history' not in result:
                raise IOError("No history found for %s. Are you sure you found a unique location?" % day_key)

            result = result.get('history').get('observations')

            # writing json cache
            cache[day_key] = result
            with open(self.cache_file, 'w') as f:
                json.dump(cache, f)
                f.close()

            sleep(6)
            return result


    def generate_rows(self, dataset_schema=None, dataset_partitioning=None,
                            partition_id=None, records_limit = -1):

        from_date = datetime.datetime.strptime(self.from_date, "%Y-%m-%d")
        to_date = datetime.datetime.strptime(self.to_date, "%Y-%m-%d")

        if to_date < from_date:
            raise ValueError("The end date must occur after the start date")

        if to_date >= datetime.datetime.today():
            raise ValueError("The end date must occurs before today")

        list_datetimes = [from_date + datetime.timedelta(days=x) for x in range((to_date-from_date).days + 1)]
        print "Wunderground plugin - List of dates: %s" % ", ".join([d.strftime("%d/%m/%Y") for d in list_datetimes])

        # Test request
        # TODO

        # Requests
        LIMIT = 5 #for debug
        i = 0
        for day in list_datetimes:
            if i >= LIMIT:
                print "Wunderground plugin - Limit per execution reached"
                break
            else:
                i += 1
                result = self.get_weather(day)
                yield {'day':day.strftime("%d/%m/%Y"),'json':json.dumps(result)}

            #### old stuff
            #elif d.strftime("%Y-%m-%d") not in existing_dates:
            #    print  "Wunderground plugin - Request for %s" % d.strftime("%d/%m/%Y")
            #    i += 1

            #else:
            #    log( "Already data %s" % d.strftime("%d/%m/%Y"))



        
