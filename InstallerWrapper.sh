#!/bin/bash
JAVA=$(which java)
export CATALINA_HOME=/opt/myqa2
del(){
cd /opt/myqa2/backup_installer
var=`du -s|awk '{print $1}'`
echo "size of backup directory is $var"
rm -fr *
echo "backup_installer cleared"
cd /opt/installer/qainstaller
}
$JAVA -jar $1 --configFile=$2 --type=fresh --force=true
sleep 2m
echo "$1 installed"
del
$JAVA -jar $3 --configFile=$4 --type=upgrade
sleep 2m
netstat -tlpn|grep java
echo "$2 upgrade installed"
#mysql jdbc and get the the number of tables
#launch myqa
#login test, version test, etc..
echo "finished execution"
