# Bigquery Dataframe plugin

BigQuery DataFrames provides a Pythonic DataFrame and machine learning (ML) API powered by the BigQuery engine. This plugin allow users to use Bigquery Dataframes using Dataiku Python Recipe and push compute to Bigquery.

## How to Use-

1. Install the Plugin in Dataiku by either cloing from Git or by uploading the plugin zip
2. Add Below packages to your Dataiku Python code env
    ``` bash
    google
    bigframes
    ```

2. Select a BQ Dataset and open a Python recipe
4. Add below code to create a bigframe object -
    ``` python
    #Load Plugin Library
    dataiku.use_plugin_libs('google-bigframes')
    #Import Dataiku Bigframe wrapper
    from dkubigframes import DkuBigframes
    #Create a bigframe object
    dku_bigframe = DkuBigframes()   
    ```
5. Create a Bigquery Dataframe object of a BQ Table (sample code) -
    ``` python
    input_dataset = dataiku.Dataset("<input_dataset_name>")
    bq_df = dku_bigframe.get_dataframe(input_dataset)
    ```
6. To write a Bigquery Dataframe to a BQ Table (sample code) - 
    ``` python
    output_dataset = dataiku.Dataset("<output_dataset_name>")
    dku_bigframe.write_with_schema(output_dataset,df)
    ```

## Known Limitations-

1. Auth Type Supported (limited to) -
    - Private Key (.p12 not supported)
    - OAuth2
2. numpy >=1.24 is required, 
    - For Dataiku < 13.2 : core packages in Dataiku python env limits numpy version to less than 1.24. In this case, uncheck the Install core packages option, copy the list of base pacakges from the code env and paste it on requested pacakges, then change the numpy version to >=1.24 and rebuild the env.

## Changelog
Initial release (v0.0.1) - Oct 14th 2024

## Need help?
Read Bigquery Dataframe APIs (https://cloud.google.com/python/docs/reference/bigframes/latest) to learn more about the supported python operations
Log a Issue on Github to request support.