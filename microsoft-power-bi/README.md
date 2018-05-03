# Microsoft Power BI


## Plugin information

This plugin provides several tools to interact with [Microsoft Power BI](https://powerbi.microsoft.com/en-us/), in its online version hosted on Azure. 

It is made of the following elements:

* a	DSS Macro to generate a Power BI Access Token (stored in the Project Variables)
* a Custom Web App Template to generate and visualize a Power BI Access Token (stored in the Project Variables)
* Two Custom Python exporters to upload DSS Datasets to Power BI (passing tbe full credentials to generate a new access token or reusing an existing token)


## Dataiku DSS and Microsoft Power BI

Microsoft Power BI and Dataiku DSS are 2 complementary solutions. Users will be able to:

* create workflows in DSS to create complex data transformation pipelines and build machine learning models, possibly relying on other Microsoft technologies (such as Azure Blob Storage, Azure Data Lake Store, Azure HDInsight or SQL Server)
* then push the outuput of these workflows directly in Power BI to be consumed by end users, using its interactive visualization and dashboarding features. 

The "Custom Exporters" can be used directly from the Export menu of the Dataset, or through an Export Recipe. If the latter is used, it will be usable in a DSS Scenario and the data could be refreshed automatically on scheduled basis. 


## Obtaining credentials to interact with the Power BI API

The process of obtaining the proper credentials to authenticate and interact with the Power BI API could be challenging. As of current version of this Plugin (1.1), you will need the following information:

* a Power BI username (i.e email address of a generic service account)
* the associated password for this user
* the Client ID of the application authorized to connect to Power BI
* the Client Secret

If you intend to use this Plugin, you will then need to have access to all these credentials. In case of issue, you may want to contact your Azure administrator. 

The main Azure documentation is there: https://docs.microsoft.com/en-us/power-bi/developer/register-app.

One example is to use the Power BI App Registration Tool:

1. In your Azure Active Directory, create a "generic" user that will be used as a service account for Power BI
2. Navigate to https://dev.powerbi.com/apps and login using the service account
3. Register a new "Server-side Web app", ticking all Dataset APIs boxes, and write down your application Client ID and Client Secret
4. Signin as the service account, go back to the Azure portal
5. In the Azure Active Directory section, go to App registrations
6. Select the App you created above (you can filter by application ID)
7. Under Required permissions, select Power BI Service, select all permissions, then click Grant permissions

You may know be able to interact with the Power BI APIs. 

You can connect to Power BI at [the following location](https://app.powerbi.com/groups/me/getdata/welcome), sigin as the service account.


## Contributing

You are welcome to contribute to this Plugin. Please feel free to use Github issues and pull requests. 
