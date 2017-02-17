# Salesforce connector

This plugin provides a read connector to interact with [Salesforce](https://www.salesforce.com) in [Dataiku DSS](http://www.dataiku.com/).

## How it works

It connects with Salesforce via the [Force.com REST API](https://developer.salesforce.com/docs/atlas.en-us.api_rest.meta/api_rest/intro_what_is_rest_api.htm).

This plugin offers two way to retrieve data:

* records of a list view [TODO]
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

The client_id is the "Consumer Key" and the client_secret is the "Consumer Secret" that you got at step 1.

You should get a JSON object like:

```{"access_token":"XXXX","instance_url":"https://XXXX.salesforce.com","id":"https://login.salesforce.com/id/XXX","token_type":"Bearer","issued_at":"1487324604890","signature":"XXX"}
```

3) Install the plugin in DSS.

4) Create a new dataset with this connector. Fill the JSON object in the required field and click on the “Test & Get schema“ button. Then, “save“ and “explore“.

## Changelog

**Version 0.0.1 "alpha" (2017-02-17)**

* Initial release
* Two datasets available: `List objects` and `SOQL query`

## Roadmap

* Debugging! Please submit feedbacks.
* Dataset for records of a list view
* Way to refresh the token
* Write connector?

## Debug

```
cat /path/to/DATA_DIR/run/backend.log | grep -i "salesforce"
```

## Ressources

* [Force.com REST API documentation](https://developer.salesforce.com/docs/atlas.en-us.api_rest.meta/api_rest/intro_what_is_rest_api.htm)
* [Plugin page](https://www.dataiku.com/community/plugins/info/salesforce.html) on Dataiku's website
* Our Q&A: [answers.dataiku.com](https://answers.dataiku.com)
* [Github issues](https://github.com/dataiku/dataiku-contrib/issues) of the repo
