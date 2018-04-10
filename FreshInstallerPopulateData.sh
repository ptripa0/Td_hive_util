#!/bin/bash
MYSQL=$(which mysql)
MYSQLDUMP=$(which mysqldump)
fromdb=$1
todb=$2
ymldirectory=$3
bindirectory=$4
master_db="$1masterdata"
$MYSQL -h localhost -u myqauser -pmyqauser@1 -e "drop database if exists $todb"
sleep 1m
echo "database $todb dropped if exists"
$MYSQL -h localhost -u myqauser -pmyqauser@1 -e "create database $todb"
sleep 1m
echo "blank database $todb created"
$MYSQLDUMP -h localhost -u myqauser -pmyqauser@1 $master_db|$MYSQL -h localhost -u myqauser -pmyqauser@1 $todb
sleep 1m
echo "database $fromdb replaced with masterdata database and renamed to $todb"
netstat -tlpn|grep java
bash $bindirectory/shutdown.sh
sleep 1m
sed -e 's/myqadb.*/'"$todb"'/g' $ymldirectory/application-dev.yml > $ymldirectory/temp.yml
mv $ymldirectory/temp.yml $ymldirectory/application-dev.yml
echo "$fromdb database now has some test data with database name as $todb"
bash $bindirectory/startup.sh
sleep 1m
netstat -tlpn|grep java
