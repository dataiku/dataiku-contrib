# Azure Data Lake Store


## Plugin information

This plugin provides a custom DSS file system provider to read data from [Azure Data Lake Store](https://azure.microsoft.com/en-us/services/data-lake-store/).

Azure Data Lake Store (ADLS) is an enterprise-wide hyper-scale repository for big data analytic workloads. Azure Data Lake enables you to capture data of any size, type, and ingestion speed in one single place for operational and exploratory analytics.


## Dataiku DSS and Azure Data Lake Store

ADLS is a fully-compatible HDFS-like file system for DSS. As such, it can be used directly with systems such [Azure HDInsight](https://azure.microsoft.com/en-us/services/hdinsight/) (which can be configured to automatically use ADLS as primary or secondary storage) or even on-premises or non-Azure managed clusters (see for instance [this blogpost](https://medium.com/azure-data-lake/connecting-your-own-hadoop-or-spark-to-azure-data-lake-store-93d426d6a5f4)).

The Plugin here does not require Hadoop or Spark integration to interact with ADLS. It addresses the simple case where DSS users simply wants to connect to ADLS, browse its directories, and read data into a regular DSS Dataset for further processing. It's a "lightweight" integration for simple use cases.

The Plugin relies on the Azure [azure-datalake-store](http://azure-datalake-store.readthedocs.io/en/latest/) Python library.


## Obtaining credentials to interact with the ADLS API

To interact with the ADLS APIs, we are using here "service-to-service" authentication. In case of questions, please refer to the [official Azure documentation](https://docs.microsoft.com/en-us/azure/data-lake-store/data-lake-store-service-to-service-authenticate-python).

To be able to authenticate against the ADLS APIs, the following credentials are required:

* a Client ID (i.e the registered App ID)
* a Client Secret (i.e the registered App secret key created in the Azure portal)
* a Tenant ID (i.e the Active Directory ID)

The App will need at least read-access on the ADLS directories you want to access.


## Using the Plugin

Once installed, the Plugin will offer a new type of Dataset available from the Dataset menu of DSS. It offers a file browser over your ADLS account, and will let you read [regular DSS file formats](https://doc.dataiku.com/dss/latest/formats/index.html).

## To do

- [ ] Implement "read" to be able to create Managed Folders on ADLS

## Contributing

You are welcome to contribute to this Plugin. Please feel free to use Github issues and pull requests. 
