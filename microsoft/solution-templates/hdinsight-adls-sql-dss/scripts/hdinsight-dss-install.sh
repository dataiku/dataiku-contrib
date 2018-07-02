#!/bin/bash -e
# Installs Dataiku Data Science Studio on an HDInsight edge node

#set -x

echo "[+] Installing Dataiku DSS HDInsight edge node"
echo "[+] Arguments: $@"

dssVersion="$1"
clusterMaster="$2"
sshUserName="$3"
sshPassword="$4"
dnsName="$5"
sqlServer="$6"
sqlUser="$7"
sqlPassword="$8"
sqlDatabase="$9"
dssDatadir=/home/dataiku/dss

echo "[+] DSS version: $dssVersion"
echo "[+] Cluster master IP: $clusterMaster"
echo "[+] SSH user name: $sshUserName"
echo "[+] SSH password: $(echo $sshPassword|sed 's/./*/g')"
echo "[+] DNS name: $dnsName"

echo "[+] Install dependencies"
apt-get -y update
apt-get -y install python python-virtualenv python-apt python-distutils-extra python-openssl nginx jq

echo "[+] Retrieve pulic IP from Azure metadata"
publicIpAddress=$(curl -s -H Metadata:true "http://169.254.169.254/metadata/instance?api-version=2017-04-02"|jq -r -C ".network.interface[0].ipv4.ipAddress[0].publicIpAddress")

echo "[+] Retrieve DSS version from website"
if [[ "$dssVersion" == "latest" ]]; then
  dssVersion=$(curl -s https://downloads.dataiku.com/latest_studio.json|jq -r '.version')
  echo "[+] Resolved DSS version: $dssVersion"
fi


echo "[+] Prepare a pyenv"
if [ ! -d ./pyenv ]; then
  virtualenv ./pyenv
  source ./pyenv/bin/activate
  pip install pip --upgrade
  pip install paramiko ansible==2.5.5 pyOpenSSL "git+https://github.com/dataiku/dataiku-api-client-python#egg=dataiku-api-client"
else
  source ./pyenv/bin/activate
fi

echo "[+] Create and install a ssh key onto the cluster master"
if [ ! -f id_install_ssh ]; then
 ssh-keygen -b 2048 -N "" -f id_install_ssh
 chmod 600 id_install_ssh id_install_ssh.pub
 python <(cat <<EOF
import paramiko
ssh_public_key=open("id_install_ssh.pub","r").readlines()[0]
print(ssh_public_key)
try:
    client = paramiko.SSHClient()
    client.load_system_host_keys()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy)
    client.connect("$clusterMaster", port=22, username="$sshUserName", password="$sshPassword")
    stdin, stdout, stderr = client.exec_command("echo '{}' >> ~/.ssh/authorized_keys".format(ssh_public_key))
    print(stdout.read())
finally:
    client.close()

EOF
)
fi


echo "[+] Generates playbook"
cat > hdiclient.yml <<EOF
- hosts: all
  remote_user: $sshUserName
  connection: local
  become: True
  vars:
    sshUser: $sshUserName
    hdiMaster: $clusterMaster
    masterIP: $clusterMaster
    currentDir: "{{ lookup('env', 'PWD') }}"
    dssVersion: $dssVersion
    dssPort: 20000
    dssDatadir: $dssDatadir
    dnsName: "$dnsName"

  tasks:
  - name: "Unprotect HDP repository file"
    file: path=/etc/apt/sources.list.d/HDP.list mode=a+rX
    delegate_to: "{{hdiMaster}}"

  - name: "retrieve HDI master facts"
    setup:
    delegate_to: "{{hdiMaster}}"
    register: hdiMasterFacts

  - name: "Register SSH infos for ansible unaware commands"
    blockinfile:
      path: /root/.ssh/config
      create: true
      block: |
        Host {{hdiMaster}}
          IdentityFile {{currentDir}}/id_install_ssh
          User {{sshUser}}
          StrictHostKeyChecking no

  - name: Retrieve HDP repository
    command: "rsync -a --delete {{sshUser}}@{{masterIP}}:/etc/apt/sources.list.d/HDP.list /etc/apt/sources.list.d/"
      
  - name: Install Hortonworks signing key
    apt_key:
      keyserver: keyserver.ubuntu.com
      id: B9733A7A07513CAD

  - name: Update package list
    apt: update_cache=yes

  # TODO compare with managed edge node
  - name: Install Hadoop packages
    apt: name={{item}}
    with_items:
    - openjdk-8-jdk
    - hadoop-client
    - hive
    - hive2
    - pig
    - tez
    - tez-hive2-*
    - spark
    - spark-python
    - spark2
    - spark2-python
    - hadooplzo
    - hadooplzo-native
    - libsnappy1
    register: install_hadoop

  - name: Clear initial Hadoop configurations
    shell: rm -rf /etc/{hadoop,hive*,pig,spark*,tez*,zookeeper}/conf
    args:
      warn: false
    when: install_hadoop.changed

  - file: path=/var/log/sparkapp state=directory mode=777 owner=root group=root

  - name: Set environment variables
    lineinfile:
      path: /etc/environment
      line: "{{item.key}}={{item.value}}"
      regexp: "^{{item.key}}="
    with_dict:
      AZURE_SPARK: 1
      SPARK_MAJOR_VERSION: 2
      PYSPARK_DRIVER_PYTHON: /usr/bin/python

  - name: Grab headnodehost definition
    command: "getent hosts headnodehost"
    delegate_to: "{{hdiMaster}}"
    register: headnodehost

  - name: Update headnodehost definition
    lineinfile:
      path: /etc/hosts
      line: "{{headnodehost.stdout_lines[0].split()[0]}} headnodehost headnodehost."
      regexp: '\sheadnodehost(\s|$)'

  - name: Synchronize HDInsight libraries
    command: rsync -a --delete {{sshUser}}@{{masterIP}}:/usr/lib/hdinsight* /usr/lib/

  - name: Synchronize Hadoop libraries
    command: rsync -a --delete {{sshUser}}@{{masterIP}}:/usr/hdp/ /usr/hdp/ --exclude=/*/oozie/ --exclude=/*/storm/

  - name: Synchronize Hadoop configurations
    command: rsync -av --delete {{sshUser}}@{{masterIP}}:/etc/{{item}} /etc/ --exclude=conf.server
    with_items:
    - hadoop
    - hive*
    - pig
    - ranger*
    - spark*
    - tez*
    - zookeeper

  - name: Create user dataiku
    become: true
    user: name=dataiku state=present

  - name: Create group dataiku
    become: true
    group: name=dataiku state=present

  - name: Create public directories
    become: true
    file:
      path: /opt/dss
      state: directory
      owner: dataiku
      group: dataiku
      mode: "u=rwx,g=rx,o=rx"

  - name: Download DSS
    become: true
    become_user: dataiku
    get_url:
      url: "https://downloads.dataiku.com/public/studio/{{dssVersion}}/dataiku-dss-{{dssVersion}}.tar.gz"
      dest: "/opt/dss/dataiku-dss-{{dssVersion}}.tar.gz"

  - name: Download SQL Server JDBC driver
    become: true
    become_user: dataiku
    get_url:
      url: "https://download.microsoft.com/download/0/2/A/02AAE597-3865-456C-AE7F-613F99F850A8/sqljdbc_6.0.8112.200_enu.tar.gz"
      dest: "/opt/dss/sqljdbc_6.0.8112.200_enu.tar.gz"

  - name: Unarchive DSS
    become: true
    become_user: dataiku
    unarchive:
      src: "/opt/dss/dataiku-dss-{{dssVersion}}.tar.gz"
      dest: /opt/dss/
      creates: "/opt/dss/dataiku-dss-{{dssVersion}}"
      remote_src: yes

  - name: Unarchive SQL Server JDBC driver
    become: true
    become_user: dataiku
    unarchive:
      src: "/opt/dss/sqljdbc_6.0.8112.200_enu.tar.gz"
      dest: /opt/dss/
      creates: "/opt/dss/sqljdbc_6.0"
      remote_src: yes

  - name: Compile DSS python code
    become: true
    become_user: dataiku
    block:
      - command: python2.7 -m compileall -q /opt/dss/dataiku-dss-{{dssVersion}}/python /opt/dss/dataiku-dss-{{dssVersion}}/dku-jupyter
      - command: python2.7 -m compileall -q /opt/dss/dataiku-dss-{{dssVersion}}/python.packages
        ignore_errors: true

  - name: Check if DSS deps have been installed
    stat:
      path: "/opt/dss/dataiku-dss-{{dssVersion}}/scripts/install/DEPS-INSTALLED"
    register: dss_deps_install_flag

  - name: Install DSS deps
    become: yes
    block:
      - shell: "/opt/dss/dataiku-dss-{{dssVersion}}/scripts/install/install-deps.sh -yes -with-r -without-java 2>&1 > /tmp/dss-install-deps.log"
      - file:
          path: "/opt/dss/dataiku-dss-{{dssVersion}}/scripts/install/DEPS-INSTALLED"
          state: touch
    when: dss_deps_install_flag.stat.exists == False

  - name: Creates DSS DATA directory
    become: true
    become_user: dataiku
    file:
      path: "{{dssDatadir}}"
      state: directory

  - name: Configure DSS instance
    become: true
    become_user: dataiku
    shell: /opt/dss/dataiku-dss-{{dssVersion}}/installer.sh -d {{dssDatadir}} -p {{dssPort}} -n -t design
    args:
      creates: "{{dssDatadir}}/dss-version.json"

  - name: Install SQL Server jdbc driver
    become: true
    become_user: dataiku
    copy:
      src: "/opt/dss/sqljdbc_6.0/enu/jre8/sqljdbc42.jar"
      dest: "{{dssDatadir}}/lib/jdbc/sqljdbc42.jar"
      remote_src: true

  - name: Set DSS channel property
    lineinfile:
      path: "{{dssDatadir}}/config/dip.properties"
      line: "dku.registration.channel=hdinsight-application"
      regexp: "^dku.registration.channel="

  - name: Set DSS XMX
    become: true
    block:
      - name: Set the XMX
        become_user: dataiku
        ini_file:
          path: "{{dssDatadir}}/install.ini"
          section: javaopts
          option: backend.xmx
          value: "6G"
        register: dss_xmx_change
      - name: Regenerate conf
        become_user: "dataiku"
        command: "{{dssDatadir}}/bin/dssadmin regenerate-config"
        when: dss_xmx_change.changed
    tags:
      - dss-set-xmx

  - name: Enable DSS service
    become: true
    command: /opt/dss/dataiku-dss-{{dssVersion}}/scripts/install/install-boot.sh {{dssDatadir}} dataiku
    args:
      creates: /etc/init.d/dataiku

  - name: Check DSS Running status
    become: true
    command: "service dataiku status"
    register: dss_instances_status
    changed_when: false
    failed_when: false

  - name: Configure DSS R integration
    become: true
    block:
      - name: Test if already installed
        become_user: "dataiku"
        stat:
          path: "{{dssDatadir}}/bin/R"
        register: dss_r_bin_installed
      - name: Stop DSS if needed
        when: not dss_r_bin_installed.stat.exists and "RUNNING" in dss_instances_status.stdout
        service:
          name: "dataiku"
          state: stopped 
      - name: Install R integration
        become_user: "dataiku"
        command: "{{dssDatadir}}/bin/dssadmin install-R-integration"
        args:
          creates: "{{dssDatadir}}/bin/R"

  - name: Check current Hadoop integration
    become: true
    become_user: "dataiku"
    command: "hadoop fs -stat /user/dataiku/dss_managed_datasets"
    register: managed_dataset_hdfs_dir
    changed_when: false
    failed_when: false

  - name: Configure HDFS
    become: true
    become_user: "dataiku"
    block:
      - name: "Make dir /user/dataiku"
        when: managed_dataset_hdfs_dir.rc is defined and managed_dataset_hdfs_dir.rc != 0
        command: "hadoop fs -mkdir -p /user/dataiku"
        environment:
          HADOOP_USER_NAME: hdfs
      - name: "Make dir /user/dataiku/dss_managed_datasets"
        when: managed_dataset_hdfs_dir.rc is defined and managed_dataset_hdfs_dir.rc != 0
        command: "hadoop fs -mkdir -p /user/dataiku/dss_managed_datasets"

  # NOTE: Already done cause we install DSS after the Hadoop client
  # Might change with a pre-built image
  #- name: Configure DSS Hadoop integration
  #  become: true
  #  block:
  #    - name: Test if already installed
  #      become_user: "dataiku"
  #      stat:
  #        path: "{{dssDatadir}}/bin/env-hadoop.sh"
  #      register: dss_hadoop_installed
  #    - name: "Stop DSS if needed"
  #      service:
  #        name: "dataiku"
  #        state: stopped 
  #      when: not dss_hadoop_installed.stat.exists and "RUNNING" in dss_instances_status.stdout
  #    - name: "Install Hadoop integration"
  #      become_user: "dataiku"
  #      when: not dss_hadoop_installed.stat.exists
  #      command: "{{dssDatadir}}/bin/dssadmin install-hadoop-integration"
  #      args:
  #        creates: "{{dssDatadir}}/bin/env-hadoop.sh"

  - name: Configure DSS Spark integration
    become: true
    block:
      - name: Test if already installed
        become_user: "dataiku"
        stat:
          path: "{{dssDatadir}}/bin/env-spark.sh"
        register: dss_spark_installed
      - name: "Stop DSS if needed"
        service:
          name: "dataiku"
          state: stopped 
        when: not dss_spark_installed.stat.exists and "RUNNING" in dss_instances_status.stdout
      - name: Run DSS Spark integration
        become_user: "dataiku"
        when: not dss_spark_installed.stat.exists 
        command: "{{dssDatadir}}/bin/dssadmin install-spark-integration"
        args:
          creates: "{{dssDatadir}}/bin/env-spark.sh"

  # NOTE: Started later on because we need to manually modify some json
  #- name: Starting DSS
  #  become: true
  #  service:
  #    name: "dataiku"
  #    state: started 

  - name: Configure nginx
    become: true
    lineinfile:
      path: /etc/nginx/nginx.conf
      insertafter: "^http {"
      state: present
      line: "    server_names_hash_bucket_size 512;"

  - name: Build a self-signed certificate
    become: true
    block:
      - name: Create Dirs
        file:
          path: "/etc/ssl/{{item}}"
          state: directory
        with_items: [ "private", "csr", "crt" ]
      - name: Download python lib
        pip: name=pyOpenSSL
      - name: Make a private key
        openssl_privatekey:
          path: "/etc/ssl/private/dss-key.pem"
      - name: Make a CSR
        openssl_csr:
          path: "/etc/ssl/csr/dss.csr"
          privatekey_path: "/etc/ssl/private/dss-key.pem"
          subject_alt_name: "DNS:{{dnsName}},IP:$publicIpAddress"
      - name: Generate a Self Signed OpenSSL certificate
        openssl_certificate:
          path: "/etc/ssl/crt/dss.crt"
          privatekey_path: "/etc/ssl/private/dss-key.pem"
          csr_path: "/etc/ssl/csr/dss.csr"
          provider: selfsigned
      - name: Save certificate name
        set_fact:
          dss_proxy_certificate: "/etc/ssl/crt/dss.crt"
          dss_proxy_private_key: "/etc/ssl/private/dss-key.pem"

  - name: Configure nginx reverse proxy
    become: true
    blockinfile:
      path: "/etc/nginx/conf.d/dss.conf"
      mode: 0644
      create: true
      block: |
        server {
            server_name {{dnsName}};
            listen 443 ssl;
            ssl_certificate {{dss_proxy_certificate}};
            ssl_certificate_key {{dss_proxy_private_key}};

            location / {
                proxy_pass http://127.0.0.1:{{dssPort}};
                proxy_redirect off;
                proxy_read_timeout 3600;
                proxy_send_timeout 600;
                proxy_http_version 1.1;
                proxy_set_header Host \$http_host;
                proxy_set_header Upgrade \$http_upgrade;
                proxy_set_header Connection "upgrade";
                client_max_body_size 0;
            }
        }
        server {
          listen 80;
          server_name {{dnsName}};
          return 302 https://\$host\$request_uri;
        }

  - name: Remove default nginx configuration
    become: true
    file: dest=/etc/nginx/conf.d/{{item}} state=absent
    with_items:
    - default.conf
    - example_ssl.conf

  - name: Restart nginx
    become: true
    service:
      name: nginx
      state: restarted
      enabled: true

EOF

echo "[+] Run playbook"
ansible-playbook -i "localhost," --ssh-extra-args "-o StrictHostKeyChecking=no" -e ansible_ssh_private_key_file=id_install_ssh hdiclient.yml


echo "[+] Update DSS configuration"
# Unfortunately, we cant use the API here since the Studio do not have a license yet
python <(cat <<EOF
# Setup hiverserver
import json
general_settings=json.load(open("$dssDatadir/config/general-settings.json","r"))
general_settings.update({
  "hiveSettings": {
      "hiveServer2Host": "headnodehost",
      "hiveServer2Port": "10001",
      "extraUrl": "transportMode=http",
  }
})
with open("$dssDatadir/config/general-settings.json","w") as settings_file:
  json.dump(general_settings,settings_file,indent=2)

# Setup a SQL Server connection
connections=json.load(open("$dssDatadir/config/connections.json","r"))
connections["connections"].update({
	"SQL_Server": {
		"params": {
			"port": -1,
			"azureDWH": False,
			"kerberosLoginEnabled": False,
			"host": "$sqlServer.database.windows.net",
			"user": "$sqlUser",
			"password": "$sqlPassword",
			"db": "$sqlDatabase",
			"useURL": False,
			"namingRule": {
				"tableNameDatasetNamePrefix": "${projectKey}_",
				"canOverrideSchemaInManagedDatasetCreation": False
			},
			"useTruncate": False,
			"autocommitMode": False,
			"properties": []
		},
		"type": "SQLServer",
		"allowWrite": True,
		"allowManagedDatasets": True,
		"allowManagedFolders": False,
		"useGlobalProxy": False,
		"maxActivities": 0,
		"credentialsMode": "GLOBAL",
		"usableBy": "ALL",
		"allowedGroups": [],
		"detailsReadability": {
			"readableBy": "NONE",
			"allowedGroups": []
		},
	}
})
with open("$dssDatadir/config/connections.json","w") as connections_file:
  json.dump(connections,connections_file,indent=2)

EOF
)

echo "[+] Restart DSS"
systemctl restart dataiku

echo "[+] Done"
