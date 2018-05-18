## Plugin information

Snowflake is a data warehouse optimized for the Cloud, faster, easier to use, and more flexible than the traditional ones. It allows corporate users to store and analyze data using cloud-based hardware and software. The data is stored in Amazon S3. 

The DSS Snowflake Plugin allows users to load data from files in an existing Amazon Simple Storage Service (Amazon S3) bucket into a new Snowflake table. That enables the processing of the data to happen directly in Snowflake.


## How It Works
 
Snowflake and S3 are complementary solutions. If you already have an AWS account and use S3 buckets for storing and managing your data files, you can make use of your existing buckets and folder paths for bulk loading into Snowflake. DSS Plugin allows you to load the data from S3 to Snowflake directly from DSS, without any external actions.

A typical usage scenario would be:

* read the input data from Amazon S3 buckets and load it to Snowflake tables by using DSS
* build workflows in DSS to create complex data transformation pipelines and build machine learning models
* push the outputs of these workflows directly to Snowflake

The Plugin allows DSS to create new tables in Snowflake and do a bulk load using the COPY INTO table command.
 
## Prerequisites

* Snowflake JDBC connection set up in DSS
* Amazon S3 connection set up in DSS
* AWS credentials: AWS Secret Key and AWS Key ID
* Snowflake Python Connector: the library ```snowflake-python-connector``` needs to be installed in the host machine. If the library does not exist, please use the following guide to install it: https://docs.snowflake.net/manuals/user-guide/python-connector-install.html 

This connector requires the following Linux libraries to exist in the host machine:

* Libssl-dev
* Libffi-dev

Check if these libraries are already installed or install them by using the following command:
```sudo apt-get install -y libssl-dev libffi-dev```


## How To Use

In order to use the Plugin:

* Connect to the preferred dataset in Amazon S3 bucket
* Add the plugin to your Flow
* Set the S3 Dataset as Input of the Plugin (mandatory - only S3 is supported)
* Assign a name for the output data which will be stored in Snowflake

The Plugin requires 2 input variables: AWS Secret Key and AWS Access Key. You can fill in the values in the plugin form or set them as local variables in your project.
 
Finally, run the Plugin Recipe and browse the data. A new table should have been created in Snowflake. 


### Error Handling

If the Plugin job fails, you can look at the error logs for the cause of the problem. Depending on the error message, the errors might be due to:

* Missing Key_ID and/or Secret_Key: “KeyError: 'AWS_KEY_ID' (or 'AWS_SECRET_KEY)”
* Wrong  Secret_Key: “SignatureDoesNotMatch”
* Wrong Key_ID: “The AWS Access Key Id you provided is not valid”
