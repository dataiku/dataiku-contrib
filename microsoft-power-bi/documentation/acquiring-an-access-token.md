# Acquiring an access token for the Azure Power BI Plugin

**Valid as of**: June 30th, 2019	
**Note**: you will need admin level privileges on your Azure subscription to complete the steps below.	

> **Important**: this documentation is provided as-is, on a best-effort basis and is meant to provide guidance
> to end users to obtain a valid access token to interact with the Power BI service. It has not been validated
> by Microsoft. The official Microsoft documentation will always prevail on this one. 

## Overview
In order to use the Power BI Plugin, you will need a proper access token to interact with the service through thier REST API's. 
The current version of the Plugin offers utilities to acquire valid credentials but this can prove challenging to get all the steps working properly. 
This document shows a procedure to acquire an access token providing with the proper credentials to use the Plugin and push a Dataiku dataset into a Power BI dataset.

For an high-level perspective, the steps involve are the following:	
* create a "service" Azure user and assign the required settings (optional)
* register an Application that will interact with the Power BI service on behalf of the user above - and set the proper permissions
* obtain an access token and make it available to the Plugin

## Create a "service" user that will interact with the Power BI service (optional)
In this step - we are creating a generic "service" user that will be granted the proper permissions to interact with the Power BI service API's.	
> **Note**:
> The Dataiku Power BI Plugin users will push their data 
> into this Power BI account workspace. You will need to connect 
> to Power BI with the associated account to see and interact with your 
> data.	

The following steps can be followed - when using the Azure Portal:	
* Log into the Azure portal, in the directory you want use 
* Click on the **Azure Active Directory** menu, then click on **Users**.
* Create a new user:
	* Click on **"+ New user"**
	* Enter a new name (ex: *PowerBI Service User for Dataiku*)
	* Enter a new username (ex: *pbi-service-user@YOUR-TENANT.com*)
	* Click **"Create"**
* Go back to **Users**, and click on the newly created user (ex: *PowerBI Service User for Dataiku*)
	* Click on **"Profile"** > **"Settings"**, and set the **Usage location** value
	* Click on **"Licenses"** > **"+ Assign"** > **"Products"** and select **"Power BI (free)"** (Make sure assignement is finalized)
	
>  **Note**:
> If you intend to use an already existing user, you should make sure that its settings have 
> at least *Usage location* and the *Power BI license* set.

## Register an App that will interact with Power BI service API's on behalf of the user
The following steps can be followed - when using the Azure Portal:	
* Click on the **Azure Active Directory** menu, then click on **App Registrations**.
* Create a new App
	* Click **"+New registration"**
	* Enter a name (ex: *Dataiku App to Push Data to PowerBI*)
	* click **"Register"**
* Set the proper authentication properties for your App
	* Click on **Authentication**
	* Add a new *Public client (mobile & desktop)* > ```https://dev.powerbi.com/Apps/SignInRedirect``` property
	* Add a new *Public client (mobile & desktop)* > ```urn:ietf:wg:oauth:2.0:oob``` property
	* Under *Implicit grant*, select **"ID tokens"**
	* Under *Default client type*, select **Yes**
	* Click **Save**
* Set the proper permissions for your App
	* Click **"+ Add a permission"**
	* Select **Power BI Service**
	* Click **"Delegated permissions"**
	* Select **Dataset** > **Dataset.ReadWrite.All**
	* Click **Add permissions**
	* Click **Grant admin consent for ....(name of the directory)"** (*Yes*)
* Set ownership of the App
	* Click **"Owner"**
	* Click **"+ Add owners** and add the user that will interact with the Power BI Service API's (ex: *PowerBI Service User for Dataiku*, as created in the previous step)
* Go back **Overview**, and write down your **Application (client) ID**


## Obtaining an access token for the Plugin
The following steps can be used to obtain a valid access token to input into the Power BI Plugin. 
* Make sure you installed the Power BI Plugin from the Dataiku plugin store. 
* Run the following Python code (in a Jupyter Notebook for instance). Note that you will need the *Adal* package, you may want to build a dedicated code env to install it.
```python
import adal
import requests

authority_url = 'https://login.windows.net/common'
resource_url = 'https://analysis.windows.net/powerbi/api'

client_id = "YOUR-APP-APPLICATION-ID"
username = "YOUR-USERNAME@YOUR-TENANT"
password = "YOUR-USERNAME-PASSWORD"

context = adal.AuthenticationContext(
    authority=authority_url,
    validate_authority=True,
    api_version=None
)

token = context.acquire_token_with_username_password(
    resource=resource_url,
    client_id=client_id,
    username=username,
    password=password
)

access_token = token.get('accessToken')
print(access_token)
```
* Copy/paste the access token in a Project Variable (or replace current value) with the following key:
```
{
  "powerbi-settings": {
    "access_token": "YOUR-ACCESS-TOKEN"
  }
}
```
> **Note**: This Project variable should be set in the Project where you intend to export data to Power BI
* Finally, when using the Plugin, choose the "... **with access token**" export options. 