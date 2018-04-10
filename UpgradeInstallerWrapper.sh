#!/bin/bash
JAVA=$(which java)
export CATALINA_HOME=/opt/myqa2
del(){
cd /opt/myqa2/backup_installer
var=`du -s|awk '{print $1}'`
echo "size of backup directory is $var"
rm -rf *
echo "backup_installer cleared"
cd /opt/installer/qainstaller
}
#sleep18
del
$JAVA -jar $1 --configFile=$2 --type=upgrade
sleep 1m
netstat -tlpn|grep java
echo "$2 upgrade installed"
#mysql jdbc and get the the number of tables
#launch myqa
#login test, version test, etc..
echo "finished execution"
