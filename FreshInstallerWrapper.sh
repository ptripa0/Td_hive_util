#!/bin/bash
JAVA=$(which java)
export CATALINA_HOME=/opt/myqa2
$JAVA -jar $1 --configFile=$2 --type=fresh --force=true
sleep 1m
echo "$1 installed"
netstat -tlpn|grep java
#mysql jdbc and get the the number of tables
#launch myqa
#login test, version test, etc..
echo "finished execution"
