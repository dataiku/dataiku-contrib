import dataiku
from dataiku.customrecipe import *
import os
import socket
import dataikuapi
# twitter client
from birdy.twitter import UserClient,TwitterApiError,ApiResponse

def getAPIUrl():
    if "dataiku_url" not in get_recipe_config():
        dku_port = os.environ['DKU_BASE_PORT']
        host = socket.gethostname()
        return 'http://'+host+':'+dku_port
    else:
        return get_recipe_config()['dataiku_url']

def getConnection(name,key):
    APIUrl = getAPIUrl()
    print "API URL: "+APIUrl
    client = dataikuapi.dssclient.DSSClient(APIUrl,key)
    return client.list_connections()[name]['params']


def get_client():
	twitter = getConnection(get_recipe_config()['connection_name'],get_recipe_config()['dataiku_token'])

	# Twitter API keys
	CONSUMER_KEY=twitter['api_key']
	#CONSUMER_SECRET=get_recipe_config()['consumer_secret']
	CONSUMER_SECRET=twitter['api_secret']
	# User Access Keys
	#ACCESS_TOKEN=get_recipe_config()['access_token']
	ACCESS_TOKEN=twitter['token_key']
	#ACCESS_TOKEN_SECRET=get_recipe_config()['access_token_secret']
	ACCESS_TOKEN_SECRET=twitter['token_secret']

	# init API client
	return UserClient(CONSUMER_KEY,CONSUMER_SECRET,ACCESS_TOKEN,ACCESS_TOKEN_SECRET)


def get_userinfo(user):
    try:
        print "Get user's info: "+str(user)
        response = client.api.users.show.get( user_id=user )
        return response
    except TwitterApiError, e:
        print "Exception: "+e._msg
        
        # set interval to 60 in case of error
        interval = 60
        print "Sleep "+str(interval)+" s"
        time.sleep(interval)
    return False



# Interval in seconds
DEFAULT_INTERVAL=int(get_recipe_config()['default_interval'])

def calc_interval(headers):
    interval = DEFAULT_INTERVAL
    # if we hit the limit, wait for resetting time
    if 'x-rate-limit-remaining' in headers and headers['x-rate-limit-remaining'] <= 0:
        current_timestamp = int(time.time())
        interval += int(headers['x-rate-limit-reset']) - current_timestamp
    return interval
