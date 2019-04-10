# HOW TO USE IT :

### This plugins requires to perform some actions in the data_dir/tmp using the Python os library. If this is not allowed the plugin won't work.
### Please, create a code-env for this plugin this will keep your libraries up to date.

## 0/ INTRO
The plugin is building the US Census from raw data.

The US Census data are available here with all the details. http://www.census.gov/. All the sources are downloaded from the US Census server and API. 
For now, the plugin only supports the American Community Survey 5 Years (ACS5Y). ACS5Y contains >40000 fields splitted in:
- Estimations: estimated value of the census question.
- Margins :margins related to the survey.

The plugin contains an option avoiding to download multiple times the resources. 
The resources are stored temporarly in the data_dir/tmp folder of DSS and removed by default at the end of the process. 
Check the options available in the "US Census sources" section of the custom dataset and recipe.
For keeping the sources and reuse it (beware of any US Census update...):
1> Uncheck the deletion option, build.
2> At the next build (of the same or another task) , check: "use previous sources".

## 1/ US CENSUS CUSTOM DATASET:
Enter your states in lower case separated by a comma with no space.
Define the level required (TRACT, COUNTY, BLOCK_GROUP)
Enter manually the Census fields you would get (ex: B00001_001E,B08534_001E). Here is the total catalog : http://api.census.gov/data/2014/acs5/variables.html 
- For a more agnostic selection, please us the "US Census feature selection recipe".


## 2/ BUILD CENSUS WITH FEATURE SELECTION:
Important: 
1/the feature selection is done state by state and not for all the states submited at the same time. 
If a feature is selected for one state, it's for all the states.
2/the feature selection only deals with binary classification or regression , not multinomial classification. Make sure that no empty, Nan, etc... is in the target field !

Nb max fields: the N top significants P-values. Default: -1 = All. Mind the volume of fields and the high correlated variables it'll generate.
Fields per output file: The number of fields per file. if 600 features have been selected for a value of 200, 3 files will be generated into the output. With a simple python, R or Spark script, you can import these data in a DSS Dataset.

Imputation: 
- since the US Census is built per state, all imputation is related to the state processed. 
- imputation is only done for feature selection, all the output data are row data.

No result:
- it is possible that the feature selection returns 0 selected features: it'll be written in the job logs.

## 3/ US CENSUS CUSTOM DATASETS 

a/STATES RESOURCES:
- show what the exact US Census state format
- https://en.wikipedia.org/wiki/Federal_Information_Processing_Standard_state_code

b/ METADATA: 
- the entire set of data and metadata for a specific US Census vintage.

A/ SOME ELEMENTS TO KNOW :

Data delivered by this plugin are raw data. All cleaning and preparation is only done for feature selection.

Some states being huge like Texas or California. The process will be longer. You should avoid building a unique census dataset containing large states, for that purpose in the feature selection recipe, set the "generate a unique file for all states" option to off.

You can check the total levels (number of blocks, tracts, ...) here : 
- https://www.census.gov/geo/maps-data/data/tallies/tractblock.html
- Nb : it makes sense if the option "Only keep census level matching input" is not checked.


A way to check the date if any doubt (beware, this is not easy to compare the data produced by the plugin):
https://censusreporter.org/data/table/?table=B00001&geo_ids=05000US10001,150|05000US10001&primary_geo_id=05000US10001

> Dimensions must be Exactly the same, any doubt download the data from census reporter and check in excel (better...)


## Requirements/recommendations/important note:

=> ! Try it on a neutral environment [dev, sandbox] before any deployment in production !
=> This plugin might not work in Multi User Security environment since it requires the Plugin to access the data_dir and uses Python system libraries.

Please create the code-env according to the definition provided with the plugin

Otherwise, without code-envs:
This plugin is using the Requests python Lib. For accessing the US Census servers Requests need some security addons that can be found in the request[security] lib. Since Requests is used by DSS, it's proposed to install the following dependencies separately:

- idna<2.6,>=2.5
- cryptography>=1.3.4
- pyopenssl>=0.14
=> These dependencies can be tricky to install on Mac OS and only due to pip, not the plugin. Mind: Mac OS not officially supported by Dataiku : https://doc.dataiku.com/dss/latest/installation/index.html

=> Please check your the results generated with the resources provided or by yourself. The plugin is delivered as-is and usage and outputs under your responsability.

