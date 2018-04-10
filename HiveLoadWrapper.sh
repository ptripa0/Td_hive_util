#!/bin/sh
JAVA=$(which java)
#loadtype="$1"
#v2=$2
case "$1" in
initial)
#initial (full) load script
echo "Starting initial (full) load sqoop Job"
echo "Starting initial load with dim_product_t7 table"
sqoop import --driver com.teradata.jdbc.TeraDriver --connect jdbc:teradata://10.200.100.52/database=qastage,CHARSET=UTF8 --username dbc --password dbc --table dim_product_t7 --m 1 --hive-import --hive-database qastage --hive-overwrite
echo "Starting initial load with order_detail_base_t7 table"
hive --hiveconf  dm.batch.id=InitialLoad_batch_id_`date +%y-%m-%d' '%H:%M:%S` -e "DROP TABLE IF EXISTS qastage.order_detail_base_t7"
sqoop import --create-hive-table --driver com.teradata.jdbc.TeraDriver --connect jdbc:teradata://10.200.100.52/database=qastage,CHARSET=UTF8 --username dbc --password dbc --table order_detail_t7 --map-column-hive order_date=date,last_update=timestamp --hcatalog-database qastage --hcatalog-table order_detail_base_t7 --create-hcatalog-table --hcatalog-storage-stanza "stored as orcfile" --split-by last_update --hive-table qastage.order_detail_base_t7
#use sqoop jobs
#call full/initial load hql
echo "Starting initial (full) load hql script"
hive --hiveconf dm.batch.id=InitialLoad_batch_id_`date +%y-%m-%d' '%H:%M:%S` -f /home/prabodh.tripathi/qa/InitialLoad.hql
echo "Initial load Scripts end here"
;; 
incremental)
#Incremental load script
echo "Starting Incremental load sqoop Job"
echo "Starting incremental load with dim_product_t7 table"
sqoop import --driver com.teradata.jdbc.TeraDriver --connect jdbc:teradata://10.200.100.52/database=qastage,CHARSET=UTF8 --username dbc --password dbc --table dim_product_t7 --m 1 --hive-import --hive-database qastage --hive-overwrite
echo "Starting incremental load with order_detail_incr_t7 table"
hive --hiveconf  dm.batch.id=IncrementalLoad_batch_id_`date +%y-%m-%d' '%H:%M:%S` -e "DROP TABLE IF EXISTS qastage.order_detail_incr_t7"
sqoop import --create-hive-table --driver com.teradata.jdbc.TeraDriver --connect jdbc:teradata://10.200.100.52/database=qastage,CHARSET=UTF8 --username dbc --password dbc --map-column-hive order_date=date,last_update=timestamp --query 'select * from qastage.order_detail_t7 where $CONDITIONS' --hcatalog-database qastage --hcatalog-table order_detail_incr_t7 --create-hcatalog-table --hcatalog-storage-stanza "stored as orcfile" --split-by last_update --hive-table qastage.order_detail_incr_t7 --where "last_update=`date`"
#use sqoop jobs
#call staging update script
#call reporting update script
echo "Starting incremental load hql script"
hive --hiveconf  dm.batch.id=IncrementalLoad_batch_id_`date +%y-%m-%d' '%H:%M:%S` -f /home/prabodh.tripathi/qa/IncrementalLoad.hql
# --hiveconf CURRENT_DATE=`date +%y-%m-%d' '%H:%M:%S`
echo "Incremental load Scripts end here"
;;
*)
echo "Incorrect arguments"
exit 1
esac
