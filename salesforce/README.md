# Salesforce connector

This plugin provides a read connector to interact with [Salesforce](https://www.salesforce.com) in [Dataiku DSS](http://www.dataiku.com/).

## How it works

It connects to Salesforce via the [Force.com REST API](https://developer.salesforce.com/docs/atlas.en-us.api_rest.meta/api_rest/intro_what_is_rest_api.htm).

This plugin offers four way to retrieve data:

* records/items of an Object
* results of a List View
* results of a [SOQL query](https://developer.salesforce.com/docs/atlas.en-us.soql_sosl.meta/soql_sosl/sforce_api_calls_soql.htm) you define
* results of a Report

The plugin also offers two ways of creating / editing data:
* Create records
* Update records

## How to set-up

1) Create a new "Connected App" in your Salesforce portal.

The documentation is [here](https://developer.salesforce.com/docs/atlas.en-us.api_rest.meta/api_rest/intro_defining_remote_access_applications.htm).

Basically, you should go in "Setup", do a "Quick Find", search for "App Manager". Then, create a "New Connected App". Name your application. Tick the box "Enable OAuth Settings". In "Selected OAuth Scopes", make all scopes available. Add a random "Callback URL" (for example: http://localhost/). Finally, save and note down the "Consumer Key" and the "Consumer Secret".

![Preview](https://raw.githubusercontent.com/dataiku/dataiku-contrib/master/salesforce/images/connectedapp2.png)

2) Install the plugin in your Dataiku DSS instance.

3) In Dataiku DSS, create a recipe "Salesforce - Refresh the JSON token".

This recipe will get and save in a file a JSON token from Salesforce API. This token is required to use any dataset of this plugin. A token expires after 1 hour, so you will probably have to run this recipe regularly.

In the recipe, you will need to provide:

* the Consumer Key and the Consumer Secret you previously got
* the Email you use to connect to the Salesforce instance
* the Password and the Security Token appended (`MyPasswordMySecurityToken`)

For the other fields, keep the default values.

![Preview of the DSS recipe](https://raw.githubusercontent.com/dataiku/dataiku-contrib/master/salesforce/images/dssrecipe.png)

Run the recipe. Check the output dataset. The status code should be `200`.

4) Create a new dataset with this connector. Define the fields if necessary. Click on the “Test & Get schema“ button. Then, “save“ and “explore“.

## Datasets available with the plugin

**Salesforce - Objects list**

This is mainly to debug and find the objects you can access via the API. Start with this one to make sure that the connection works.

**Salesforce - SOQL query**

Get the results of a defined SOQL query.

**Salesforce - Object records**

Get the records of an Object.

An SOQL query is built to query all fields of the object.

**Salesforce - List View results**

Get the results of a List View on an object.

There are two parameters to define that you can find in the URL when you browse a List View on the portal.

Example: `https://eu11.lightning.force.com/one/one.app#/sObject/Opportunity/list?filterName=00B0Y000004Fdd1UAC&a:t=1488295234124`

* Object name: `Opportunity`
* List View id: `00B0Y000004Fdd1UAC`

![Preview](https://raw.githubusercontent.com/dataiku/dataiku-contrib/master/salesforce/images/listviewurl.png)

**Salesforce - Report (BETA)**

Get the results of a Report.

According to [the documentation](https://developer.salesforce.com/docs/atlas.en-us.api_analytics.meta/api_analytics/sforce_analytics_rest_api_getreportrundata.htm), the report is run immediately and returns the latest summary data for your level of access.

Two restrictions: the report must be in TABULAR format and must be available with a synchronous API call.

## Recipes available with the plugin

** Salesforce - Create Objects (BETA)
A recipe that let's you create Salesforce object records based on the columns of a dataset, columns should have the same name as your Salesforce fields name

** Salesforce - Update Objects (BETA)
A recipe that let's you update Salesforce object records based on the columns of a dataset, columns should have the same name as your Salesforce fields name

## Changelog

** Version 1.2.0 (2018-06-11) **
*New : Create and Update records recipes added

**Version 1.1.0 (2017-12-01)**

* Fixed: the number of rows returned by the plugin is now enforced (that fixes an issue with DSS 4.1.0)
* New: Report dataset (beta)

**Version 1.0.0 (2017-06-12)**

* Enhanced: the SOQL query field is now multi-line
* More consistency in the naming of the python-connectors

**Version 0.1.1 "beta 2" (2017-04-18)**

* Fixed: Schema of the output of datasets
* Enhanced: DSS shows an error when not able to refresh the JSON token (recipe)

**Version 0.1.0 "beta 1" (2017-04-03)**

* New: Recipe to refresh the token
* New: The token can be stored in a file. This way, it can be shared by all datasets.
* New: Dataset to get records of an Object
* Enhanced: Clean null values when output format is 'Readable with columns'

**Version 0.0.2 "alpha 2" (2017-02-28)**

* New dataset available: `List View records` and `SOQL query`
* Improved documentation

**Version 0.0.1 "alpha 1" (2017-02-17)**

* Initial release
* Two datasets available: `List objects` and `SOQL query`

## Roadmap

* Debugging! Please submit feedbacks.
* Support of another OAuth Authentication Flow (with a refresh token)
* Write connector?
* Report: support of other formats (summary and matrix reports) and asynchronous calls

## Alternative configuration

You can obtain the JSON token outside of Dataiku DSS (step 3 of the set-up). Basically, you need to authenticate with the [Username-Password OAuth Authentication Flow](https://developer.salesforce.com/docs/atlas.en-us.api_rest.meta/api_rest/intro_understanding_username_password_oauth_flow.htm). You store the JSON in a file and you reference this file in the datasets settings.

For example, you can run the following Python code in a cron task.

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
with open('/path/for/my/SalesforceToken.json', 'w') as f:
    f.write(r.text)
    f.close()
```

You should get a JSON object like this:

```
{"access_token":"XXXX","instance_url":"https://XXXX.salesforce.com","id":"https://login.salesforce.com/id/XXX","token_type":"Bearer","issued_at":"1487324604890","signature":"XXX"}
```

## Troubleshooting

**How to get logs?**

Have a look at DSS logs: `backend.log`

You might want to filter:

```
cat /path/to/DATA_DIR/run/backend.log | grep -i "salesforce"
```

**When trying to authenticate, I get `{"error":"invalid_grant","error_description":"authentication failure"}`**

Make sure that:

* Your Security Token is still valid.
* IP restrictions are disabled (or not blocking your IP).
* Permitted Users is set to "All users may self-authorize".
* You're not using a TLS 1.0 or below. You can debug this with Python script: `requests.get("https://www.howsmyssl.com/a/check").text`

## Resources

* [Force.com REST API documentation](https://developer.salesforce.com/docs/atlas.en-us.api_rest.meta/api_rest/intro_what_is_rest_api.htm)
* [Plugin page](https://www.dataiku.com/community/plugins/info/salesforce.html) on Dataiku's website
* Our Q&A: [answers.dataiku.com](https://answers.dataiku.com)
* [Github issues](https://github.com/dataiku/dataiku-contrib/issues) of the repo