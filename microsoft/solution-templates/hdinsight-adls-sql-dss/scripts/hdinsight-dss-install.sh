#!/bin/bash -e
# Installs Dataiku Data Science Studio on an HDInsight edge node

echo "+ Installing Dataiku DSS HDInsight edge node"
echo "+ Arguments: $@"
echo "+ Arguments: $@" > /tmp/debug.log

exit 0


Usage() {
	echo >&2 "Usage: $0 [-version DSS_VERSION] -port PORT"
	exit 1
}

version=
port=
while [ $# -gt 0 ]; do
	if [ $# -ge 2 -a "$1" = "-version" ]; then
		version="$2"
		shift 2
	elif [ $# -ge 2 -a "$1" = "-port" ]; then
		port="$2"
		shift 2
	elif [ "$1" = "-n" ]; then
		echo "Dryrun only"
		exit
	else
		Usage
	fi
done
if [ -z "$port" ]; then
	Usage
fi

user="dataiku"
dataDir="dss_data"

# Retrieve DSS version if needed
if [ -z "$version" -o "$version" = "latest" ]; then
	echo "+ Retrieving latest DSS version from dataiku.com"
	version=$(curl -sS http://downloads.dataiku.com/public/hdi-app/latest_studio.json |
		python -c 'import json, sys;print json.load(sys.stdin)["version"]')
	if [ -z "$version" ]; then
		echo >&2 "* Error: could not retrieve latest DSS version"
		exit 1
	else
		echo "+ Using DSS version $version"
	fi
fi

# Create user if needed
if getent passwd "$user" >/dev/null; then
	echo "+ User $user already exists"
else
	echo "+ Creating user $user"
	useradd -m "$user"
fi
userDir=$(getent passwd "$user" | awk -F : '{print $6}')

# Create HDFS user directory if needed
echo "+ Creating HDFS home dir for user $user"
su - hdfs -c "hdfs dfs -mkdir -p /user/$user && hdfs dfs -chown $user:$user /user/$user"

# Download and extract installation kit if needed
kitName="dataiku-dss-$version"
su - "$user" -c "
set -e
if [ -e '$kitName/installer.sh' ]; then
	echo '+ Installation kit $kitName already exists'
else
	echo '+ Downloading installation kit'
	rm -f $kitName.tar.gz
	curl -OsS https://downloads.dataiku.com/public/studio/$version/$kitName.tar.gz
	echo '+ Extracting kit'
	tar xf $kitName.tar.gz
fi
"

# Install DSS dependencies
echo "+ Installing DSS dependencies"
apt-get update
"$userDir/$kitName/scripts/install/install-deps.sh" -without-java -with-r -yes

# Install DSS if needed
su - "$user" -c "
set -e
if [ -e '$dataDir/dss-version.json' ]; then
	echo '+ DSS already installed'
else
	echo '+ Installing DSS'
	set -x
	$kitName/installer.sh -d '$dataDir' -p '$port'
	echo -e '\ndku.registration.channel=hdinsight-application' >>'$dataDir'/config/dip.properties
fi
echo '+ Configuring Nginx for HDI'
python2.7 -c '
import ConfigParser
iniFile = \"$dataDir/install.ini\"
config = ConfigParser.RawConfigParser()
config.read(iniFile)
config.set(\"server\", \"websocket_permessage_deflate\", \"false\")
with open(iniFile, \"w\") as f:
	config.write(f)
'
echo '+ Configuring Hiveserver2 URL'
python2.7 -c '
import json, sys
with open(\"$dataDir/dss-version.json\") as f:
	dssVersion = json.load(f)[\"product_version\"]
if int(dssVersion.split(\".\")[0]) < 4:
	print \"- DSS version < 4, skipping\"
	sys.exit()
gsFile = \"$dataDir/config/general-settings.json\"
with open(gsFile) as f:
	gs = json.load(f)
gs[\"hiveSettings\"][\"hiveServer2Host\"] = \"headnodehost\"
gs[\"hiveSettings\"][\"hiveServer2Port\"] = \"10001\"
gs[\"hiveSettings\"][\"extraUrl\"] = \"transportMode=http\"
with open(gsFile, \"w\") as f:
	json.dump(gs, f, indent=2)
'
echo '+ Installing DSS R integration'
$dataDir/bin/dssadmin install-R-integration
if command -v spark-submit >/dev/null; then
	if [ -n \"\$SPARK_HOME\" ]; then
		echo '+ Installing DSS Spark integration, sparkHome='\"\$SPARK_HOME\"
		$dataDir/bin/dssadmin install-spark-integration -sparkHome \"\$SPARK_HOME\"
	else
		echo '+ Installing DSS Spark integration'
		$dataDir/bin/dssadmin install-spark-integration
	fi
	echo '+ Installing DSS H2O integration'
	$dataDir/bin/dssadmin install-h2o-integration
else
	echo '+ Spark is not available'
fi
"

# Configure and start the service
echo "+ Installing service"
"$userDir/$kitName/scripts/install/install-boot.sh" "$userDir/$dataDir" "$user"
echo "+ Starting service"
service dataiku start

echo "+ Done"
