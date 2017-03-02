# Dataiku DSS on a HDInsight edge node

<a href="https://portal.azure.com/#create/Microsoft.Template/uri/https%3A%2F%2Fraw.githubusercontent.com%2FThomasCabrol%2Fdataiku%2Fmaster%2Fpartners%2Fmicrosoft%2Fhdinsights%2Fdataiku-dss%2Fazuredeploy.json" target="_blank">
    <img src="http://azuredeploy.net/deploybutton.png"/>
</a>

### Summary
This template adds an edge node to an existing HDInsight cluster, then installs Dataiku DSS on the edge node, pre-configured to work with the cluster

### Dataiku DSS
<a href="http://www.dataiku.com/dss/">Dataiku DSS</a> is a collaborative data science software platform that enables teams to explore, prototype, build, and deliver their own data products more efficiently.
Dataiku DSS is:
* an all-in-one tool to build your project end-to-end, from preparation to deployment.
* a common platform to bring together data scientists, analysts, and data ops alike.
* a software to help leverage and scale your existing infrastructure: whether SQL data warehouse or Spark cluster.
* an environment to promote the coexistence of all standard (big) data science technologies and languages.
Dataiku DSS offers a complete set of functionalities to access, manage, wrangle, visualize, analyze and predict data, no matter their format and sources. 

### Dataiku DSS and HDInsight
Dataiku DSS is the perfect companion of your HDInsight cluster, thanks to the following features:
* get your data in or out the HDInsight cluster transparently
* access, transform, analyze and visualize your HDFS datasets
* use "Visual Recipes" to perform common and advanced data wrangling tasks with no code, and automatically turned them into MapReduce or Spark jobs pushed to the underlying HDInsight cluster
* use "Coding Recipes" to express custom business logic and perform advanced analytics using MR-based (Hive, Pig) or Spark-based (Scala, PySpark, SparkSQL or SparkR) layers
* create supervised or unsupervised machine learning models in a few clicks using MLLib or H2O
* interactively analyze large datasets using Jupyter Notebooks
In any case, Dataiku DSS will leverage the underlying HDInsight cluster as much as possible to ensure maximum scalability and performance. 

Want to know more? Feel free to <a href="http://www.dataiku.com/">visit our website</a> or to <a href="mailto:contact@dataiku.com">send us an email.</a>. 