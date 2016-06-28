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

        self.api_key = str(self.config.get("api_key"))
        self.latitude = str(self.config.get("latitude"))
        self.longitude = str(self.config.get("longitude"))
        self.from_date = str(self.config.get("from_date"))
        self.to_date = str(self.config.get("to_date"))
        self.cache_folder = str(self.config.get("cache_folder"))
        self.api_limit = int(self.config.get("api_limit", -1))

        # Cache file path
        filename = "cache-forecastio-history-%s.json" % base64.urlsafe_b64encode(str(self.latitude) + '-' + str(self.longitude))
        self.cache_file = os.path.join(self.cache_folder, filename)

        # Cache directory
        if not os.path.isdir(self.cache_folder):
            os.makedirs(self.cache_folder)

        # Create cache file if does not exist
        if not os.path.exists(self.cache_file):
            with open(self.cache_file, 'w') as f:
                json.dump({}, f)
                f.close()

        # The API returns the number of call made for today. We keep it to optionnaly limit the number of calls.
        # For te first call, we don't know the actual value but we assume it is 0.
        self.api_calls = 0

    def get_read_schema(self):

        return {
                "columns" : [
                    { "name" : "day", "type" : "string" },
                    { "name" : "day_date", "type" : "date" },
                    { "name" : "daily_data", "type" : "object" },
                    { "name" : "hourly_data", "type" : "object" },
                    { "name" : "full_json", "type" : "object" }
                ]
            }


    def get_weather(self, day=None):

        if not day:
            return None

        # Reading json cache
        with open(self.cache_file, 'r') as f:
            cache = json.load(f)
            f.close()

        day_key = day.strftime("%Y-%m-%dT00:00:00")

        if day_key in cache.keys():

            # In cache
            print  "Forecast.io plugin - Already in cache for %s" % day_key
            return cache.get(day_key)

        else:

            # Not in cache -> API request
            print  "Forecast.io plugin - Request for %s" % day_key

            # checking limits
            if self.api_limit > -1 and self.api_calls >= self.api_limit:
                print  "Forecast.io plugin - Limit reached, no call for %s (cur=%d lim=%d)" % (day_key, self.api_calls, self.api_limit)
                return {"result" : "Limit reached. No call." }

            # request
            headers = {
                "Accept-Encoding": "gzip",
                "Accept": "application/json"
            }
            params = {
                "units": "auto",
                "exclude": "currently,minutely"
            }
            r = requests.get('https://api.forecast.io/forecast/' + self.api_key + '/' + self.latitude + ',' + self.longitude + ',' + day_key, params=params, headers=headers)
            
            # verification of the status code
            if r.status_code != 200:
                print "Forecast.io plugin - Error in request (status code: %s)" % r.status_code
                print "Forecast.io plugin - Response: %s" % r.text
                r.raise_for_status()
                sys.exit()

            # results
            result = r.json()
            self.api_calls = r.headers.get('X-Forecast-API-Calls')
            print "Forecast.io plugin - X-Forecast-API-Calls: %s" % self.api_calls

            if self.api_calls is not None:
                self.api_calls = int(self.api_calls)
            else:
                self.api_calls = -1

            # writing json cache
            cache[day_key] = result
            with open(self.cache_file, 'w') as f:
                json.dump(cache, f)
                f.close()

            sleep(0.1)
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
        print "Forecast.io plugin - List of dates: %s" % ", ".join([d.strftime("%d/%m/%Y") for d in list_datetimes])

        # Test request
        # TODO

        # Requests
        for day in list_datetimes:
                result = self.get_weather(day)
                yield {
                        'day': day.strftime("%Y-%m-%d"),
                        'day_date' : day.strftime("%Y-%m-%dT00:00:00.000Z"),
                        'daily_data': json.dumps(result['daily']['data'][0]) if 'daily' in result and 'data' in result['daily'] else '',
                        'hourly_data': json.dumps(result.get('hourly', '')) if 'hourly' in result else '',
                        'full_json': json.dumps(result)
                    }


