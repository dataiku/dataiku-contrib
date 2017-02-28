# Salesforce connector

This plugin provides a read connector to interact with [Salesforce](https://www.salesforce.com) in [Dataiku DSS](http://www.dataiku.com/).

## How it works

It connects with Salesforce via the [Force.com REST API](https://developer.salesforce.com/docs/atlas.en-us.api_rest.meta/api_rest/intro_what_is_rest_api.htm).

This plugin offers two way to retrieve data:

* records of a List View
* results of a [SOQL query](https://developer.salesforce.com/docs/atlas.en-us.soql_sosl.meta/soql_sosl/sforce_api_calls_soql.htm) you define

## How to set-up

1) Create a new "Connected App" in your Salesforce portal.

The documentation is [here](https://developer.salesforce.com/docs/atlas.en-us.api_rest.meta/api_rest/intro_defining_remote_access_applications.htm).

Basically, you should go in "Setup", do a "Quick Find", search for "App Manager". Then, create a "New Connected App". Name your application. Tick the box "Enable OAuth Settings". In "Selected OAuth Scopes", make all scopes available. Add a random "Callback URL" (for example: http://localhost/). Finally, save and note down the "Consumer Key" and the "Consumer Secret".

![Preview](https://raw.githubusercontent.com/dataiku/dataiku-contrib/master/salesforce/images/connectedapp2.png)

2) Get a JSON token with the [Username-Password OAuth Authentication Flow](https://developer.salesforce.com/docs/atlas.en-us.api_rest.meta/api_rest/intro_understanding_username_password_oauth_flow.htm)

Python code to get it:

```
import requests
params = {
    "grant_type": "password",
    "client_id": "XXX.YYY",
    "client_secret": "0000000000000000",
    "username": "my@email.com",
    "password": "MyPasswordMySecurityToken"
}
r = requests.post("https://login.salesforce.com/services/oauth2/token", params=params)
print r.text
```

Note that you need to append your security token to your password.

The client_id is the "Consumer Key" and the client_secret is the "Consumer Secret" that you got at step 1.

You should get a JSON object like this:

```
{"access_token":"XXXX","instance_url":"https://XXXX.salesforce.com","id":"https://login.salesforce.com/id/XXX","token_type":"Bearer","issued_at":"1487324604890","signature":"XXX"}
```

3) Install the plugin in DSS.

4) Create a new dataset with this connector. Enter the JSON you got in step 3 in the "JSON token" field and define the other fields if present. Click on the “Test & Get schema“ button. Then, “save“ and “explore“.

## Datasets available with the plugin

**Salesforce - Objects list**

This is mainly to debug and find the objects you can access via the API. Start with this one to make sure that the connection works.

**Salesforce - SOQL query**

Get the results of a defined SOQL query.

**Salesforce - List View records**

Get the records of a List View on an object.

There are two parameters to define that you can find in the URL when you browse a List View on the portal.

Example: `https://eu11.lightning.force.com/one/one.app#/sObject/Opportunity/list?filterName=00B0Y000004Fdd1UAC&a:t=1488295234124`

* Object name: `Opportunity`
* List View id: `00B0Y000004Fdd1UAC`

![Preview](https://raw.githubusercontent.com/dataiku/dataiku-contrib/master/salesforce/images/listviewurl.png)

## Changelog

**Version 0.0.2 "alpha 2" (2017-02-28)**

* New dataset available: `List View records` and `SOQL query`
* Improved documentation

**Version 0.0.1 "alpha 1" (2017-02-17)**

* Initial release
* Two datasets available: `List objects` and `SOQL query`

## Roadmap

* Debugging! Please submit feedbacks.
* Way to refresh the token
* Write connector?

## Debug

```
cat /path/to/DATA_DIR/run/backend.log | grep -i "salesforce"
```

## Resources

* [Force.com REST API documentation](https://developer.salesforce.com/docs/atlas.en-us.api_rest.meta/api_rest/intro_what_is_rest_api.htm)
* [Plugin page](https://www.dataiku.com/community/plugins/info/salesforce.html) on Dataiku's website
* Our Q&A: [answers.dataiku.com](https://answers.dataiku.com)
* [Github issues](https://github.com/dataiku/dataiku-contrib/issues) of the repo
