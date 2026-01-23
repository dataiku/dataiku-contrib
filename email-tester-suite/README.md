# Email Tester Suite

This plugin provides recipes to test and validate email addresses in datasets

You probably have datasets containing email addresses, and you would like to make sure that there is a person behind these emails. The reasons behind that could be to save some emailing credits, or to avoid to be blacklisted by major mail providers if you have a poor deliverability.

This plugin offers a suite of recipes to test, validate and sometimes correct emails that are in your datasets.

As of version 0.0.2, the plugin contains two recipes:

Email address tester: this recipe verifies your email (without any API or service required) with 3 different tests:

- Format validation: make sure the emails have a valid format
- Disposable domain detection: detect the providers of free disposable email addresses (mailinator.com, …)
- MX record domain detection: detect the domains that cannot receive emails (or domains that does not exist)

Email validation via Mailgun API: with this recipe, you can use the free [Email Validation APi provided by Mailgun](https://documentation.mailgun.com/api-email-validation.html#email-validation) to validate an address based on:

- Syntax checks (RFC defined grammar)
- DNS validation
- Spell checks
- Email Service Provider (ESP) specific local-part grammar (if available).

## How To Use

The recipes can be quite long to run as API call or DNS validation can be made for each email or domain in the datasets. Note that DSS must have an access to the Internet. The plugin will cache these calls so that it will be faster on a second run.

To use the Mailgun API, you need a Mailgun public key, available in the My Account tab of the Control Panel.
