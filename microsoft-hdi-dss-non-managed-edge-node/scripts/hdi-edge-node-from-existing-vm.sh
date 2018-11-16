#!/bin/bash

Usage() {
  echo ""
  echo >&2 "Usage: $0 -headnode_ip <HEADNODE_IP> -headnode_user <HEADNODE_USER> [-n]"
  echo "'headnode_ip' is the private IP of the HDI cluster headnode"
  echo "'headnode_user' is the Linux account of the HDI cluster"
  echo "options:"
  echo "  -n: for dry-run"
  return 1
}

if [ $# -le 1 ]; then
  Usage
  return 1
fi

headnode_ip=
headnode_user=
HEADNODEHOST="headnodehost"
HOSTSFILE="/etc/hosts"

while [ $# -gt 0 ]; do
    if [ $# -ge 2 -a "$1" = "-headnode_ip" ]; then
        headnode_ip="$2"
        shift 2
    elif [ $# -ge 2 -a "$1" = "-headnode_user" ]; then
        headnode_user="$2"
        shift 2
    elif [ "$1" = "-n" ]; then
        echo "Dryrun only"
        return
    else
        Usage
	return 1
    fi
done

if [ -z "headnode_ip" ]; then
    Usage
    return 1
fi
if [ -z "headnode_user" ]; then
    Usage
    return 1
fi

PRIVATEKEYPATH=$(realpath ~/.ssh/id_rsa)
if [ ! -f $PRIVATEKEYPATH ]; then
  echo "No private key found"
  return 1
fi

sshTest=$(ssh -q -q -o StrictHostKeyChecking=no -o ConnectTimeout=3 -o PasswordAuthentication=no $headnode_user@$headnode_ip echo "OK")

if [[ $sshTest == "OK" ]]; then
  echo "SSH connection working"
else
  echo "SSH connection $headnode_user@$headnode_ip failed"
  echo "Possible couses: headnode not in the same VNET, Passwordless connection not configured"
  return 1
fi

headnodeTest=$(ssh -q -q -o StrictHostKeyChecking=no -o ConnectTimeout=3 -o PasswordAuthentication=no $headnode_user@$headnode_ip hostname -s)
if [[ $headnodeTest == hn* || $headnodeTest == headnode* ]]; then
  echo "headnode ip: $headnode_ip is headnode $headnodeTest"
else
  echo "[ERROR] headnode ip: $headnode_ip does not appear to be headnode, hostname returns $headnodeTest"
  return 1
fi

echo "Adding headnode IP to hosts with headnodehost"

if [ -n "$(grep -P "$HEADNODEHOST" $HOSTSFILE)" ]; then
  echo "Found existing entry with headnodehost ip in $HOSTSFILE deleting"
  sudo sed -i "/$HEADNODEHOST/d"  $HOSTSFILE
fi

echo "Adding new entry to $HOSTSFILE for headnodehost"
echo -e "$headnode_ip\t$HEADNODEHOST" | sudo tee -a $HOSTSFILE



testIfSudoer=$(ssh -o StrictHostKeyChecking=no -o ConnectTimeout=3 -o PasswordAuthentication=no $headnode_user@$headnode_ip 'sudo -n true; echo $?')
if [[ $testIfSudoer != 0 ]]; then
  echo "[ERROR] User $headnode_user is not sudoer on headnode $headnode_ip"
  return 1
fi

ssh -o StrictHostKeyChecking=no $headnode_user@$headnode_ip 'sudo chmod -R +r /etc/apt/sources.list.d'

sudo rsync -av -e "ssh -o StrictHostKeyChecking=no -i $PRIVATEKEYPATH" $headnode_user@$headnode_ip:/etc/apt/sources.list.d/ /etc/apt/sources.list.d/

sudo apt-key adv --keyserver keyserver.ubuntu.com --recv 07513CAD
sudo apt-get update

declare -a hadoopBasePackages=(
  "openjdk-8-jdk"
  "libsnappy1 liblzo2-2"
  "hadoop hadoop-client hadoop-hdfs hadoop-mapreduce hadoop-yarn hadooplzo hadooplzo-native hadoop-lzo"
  "hive hive2 knox pig tez tez-hive2.*"
  "spark spark2 spark-python"
)

for item in ${hadoopBasePackages[@]}
do
  sudo apt-get install -y --allow-unauthenticated $item
done


echo "[+] Synchronizing additional /usr libraries "
sudo rsync -av -e "ssh -o StrictHostKeyChecking=no -i $PRIVATEKEYPATH" $headnode_user@$headnode_ip:/usr/hdp/ /usr/hdp/
sudo rsync -av -e "ssh -o StrictHostKeyChecking=no -i $PRIVATEKEYPATH" $headnode_user@$headnode_ip:/usr/lib/hdinsight\* /usr/lib/
sudo rsync -av -e "ssh -o StrictHostKeyChecking=no -i $PRIVATEKEYPATH" $headnode_user@$headnode_ip:/usr/lib/ambari\* /usr/lib/
sudo rsync -av -e "ssh -o StrictHostKeyChecking=no -i $PRIVATEKEYPATH" $headnode_user@$headnode_ip:/usr/lib/ams\* /usr/lib/

declare -a hadoopBaseServices=(
  "hadoop"
  "hive"
  "pig"
  "spark"
  "tez"
  "zookeeper"
)

echo "Removing and synchronizing hadoopBaseServices configuration from headnode"

for service in ${hadoopBaseServices[@]}
do
  reg="/etc/$service.*/conf"
  echo "Removing configuration for service $service maching $reg"
  sudo find /etc -regex $reg | xargs sudo rm -rf
  echo "Synchronizing configuration for service $service"
  sudo rsync -av -e "ssh -o StrictHostKeyChecking=no -i $PRIVATEKEYPATH" $headnode_user@$headnode_ip:/etc/${service}\* /etc/
done

echo "[+] Adding directories for Spark logging"
sudo su -c "
  mkdir -p /var/log/spark2 && chmod 777 /var/log/spark2
  mkdir -p /var/log/sparkapp && chmod 777 /var/log/sparkapp
  mkdir -p /var/run/spark2 && chmod 777 /var/run/spark2
"

echo "[+] Exporting system variables"
export AZURE_SPARK=1
export SPARK_MAJOR_VERSION=2
export PYSPARK_DRIVER_PYTHON=/usr/bin/python


