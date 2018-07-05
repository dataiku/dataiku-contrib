## Plugin information

[Snowflake](https://www.snowflake.net/) is a data warehouse built for the Cloud offering the following characteristics:

* Performance: Snowflake easily scales to multiple petabytes and performs up to 200x faster than other systems.
* Concurrency: multiple groups can access the same data at the same time without impacting performance. 
* Simplicity: a fully managed, pay-as-you-go solution that stores, integrates and analyzes all your data.

Snowflake is built on top of the Amazon Web Services (AWS) cloud and has deep integrations with S3. Snowflake has the ability to quicly read and write data stored in S3. 

The purpose of this Plugin is to leverage these integrations to speed up data transfers and processing. 


## How It Works
 
Snowflake and S3 are deeply integrated solutions. If you already have an AWS account and use S3 buckets for storing and managing your data, you can make use of your existing buckets and folder paths for bulk loading into Snowflake. This DSS Plugin allows you to load the data from S3 to Snowflake directly, without any external actions - and ensuring fast data transfers.

A typical usage scenario would be:

* read some input data from Amazon S3 and load it to Snowflake tables by using this DSS Plugin
* build workflows in DSS to create complex data transformation pipelines (including features engineering) supported by Snowflake and build machine learning models
* push the outputs of these workflows directly to Snowflake

The Plugin allows DSS to create new Snowflake tables and to perform a fast bulkload using the COPY INTO table command from S3 data.
 
## Prerequisites

* [Snowflake (JDBC) Connection](https://doc.dataiku.com/dss/latest/connecting/sql/snowflake.html) set up in DSS
* [Amazon S3 Connection](https://doc.dataiku.com/dss/latest/connecting/s3.html) set up in DSS
* the corresponding AWS credentials for the S3 buckets (AWS Access Key and AWS Secret Key), or [temporary credentials](https://docs.aws.amazon.com/IAM/latest/UserGuide/id_credentials_temp_use-resources.html) configured.

The Plugin comes with a code environment that installs the [Snowflake Python Connector](https://docs.snowflake.net/manuals/user-guide/python-connector.html) and is automatically installed with the Plugin. Note that this connector may require the following Linux libraries to exist in the host machine:

* Libssl-dev
* Libffi-dev

Check if these libraries are already installed or install them by using the following command:
```sudo apt-get install -y libssl-dev libffi-dev```

Finally, the Plugin has been tested with Python 3.6 and requires a valid Python 3.6 installation on the machine (the Plugin code environment is restricted to Python 3.6).


## How To Use

In order to use the Plugin:

* Defined a DSS S3 Dataset
* Add the Plugin to your Flow
* Set the S3 Dataset as Input of the Plugin (mandatory - only S3 is supported)
* Assign a name for the output Dataset, stored in your Snowflake Connection

If IAM temporary credentials are not available, the Plugin requires 2 input parameters: the AWS Access Key and AWS Secret Key. You can either:

* fill in the values in the Plugin form 
* or set them Project Variables
* or set the in Global Variables

When DSS Variables are used, DSS will look for the following inputs:
```
{
  "snowflake": {
    "aws_access_key": "your-aws-access-key",
    "aws_secret_key": "your-aws-secret-key"
  }
}
```
 
Finally, run the Plugin Recipe and browse the output Dataset. A new table should have been created in Snowflake. 


### Error Handling

If the Plugin job fails, you can look at the error logs for the cause of the problem. Depending on the error message, the errors might be due to:

* Missing Key_ID and/or Secret_Key: “KeyError: 'AWS_KEY_ID' (or 'AWS_SECRET_KEY)”
* Wrong  Secret_Key: “SignatureDoesNotMatch”
* Wrong Key_ID: “The AWS Access Key Id you provided is not valid”
