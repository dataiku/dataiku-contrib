# Description

Plugin that creates and attach to an HDI cluster

* State: **Highly experimental** and __in development__

# Pre-requisites

You need DSS to run on a VM that is compatible and configured to run with HDI.

Follow the [following post](https://www.microsoft.com/developerblog/2018/08/20/attaching-and-detaching-an-edge-node-from-a-hdinsight-spark-cluster-when-running-dataiku-data-science-studio-dss/) for details


# Limitations

* HDI 3.6 with Spark 2.3 only
* Non secured cluster
* Wasb storage only
* No cluster upscaling
* Force use of HS2


