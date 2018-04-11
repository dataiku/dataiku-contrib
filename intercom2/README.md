# Intercom plugin

This plugin provides a read connector to interact with [Intercom](https://www.intercom.com), a SaaS customer messaging platform, in [Dataiku DSS](http://www.dataiku.com/dss/). You can import users, leads, companies, admins, conversations, tags, segments through Segment API.

When available, it will use the `scroll` endpoint (for example, see the documentation for [/users/scroll](https://developers.intercom.com/reference#iterating-over-all-users)).

## How to set-up

* Get an Intercom [Access Token](https://developers.intercom.com/v2.0/docs/personal-access-tokens) with Extended Scopes.
* Install the plugin in DSS.
* Create a new dataset with this connector. Fill the “Access Token“ and the other fields. Click on the “Test & Get schema“ button. Then, “save“ and “explore“.

## Changelog

**Version 1.0.0 (2018-??-??)**

* Initial release

## Need help?

Find out more on the [plugin page](https://www.dataiku.com/community/plugins/info/intercom.html) on Dataiku's website.

Ask your question on [answers.dataiku.com](https://answers.dataiku.com). Or, [open an issue](https://github.com/dataiku/dataiku-contrib/issues).
