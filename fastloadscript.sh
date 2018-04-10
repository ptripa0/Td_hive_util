#!/bin/sh
#This sh is developed using Terada fastload utility to load empty staging table
#----------define input and log files--------------#
Datafile=$1
inputf=/root/pelican_qa/$Datafile
echo $inputf
logf=/root/pelican_qa/dim_product_t.log
echo $logf
#-----------file exist or not----------------------#
if test "$inputf" = "/root/pelican_qa/dim_product_t_1.txt"
then 
echo "file $inputf exists"
else 
echo "file $inputf does not exist"
exit 1
fi
#----------print date --------#
date2=$date
echo $date2
echo "$`date`"
echo "Date is `date`"
#--------------------fast load -----------------------#
fastload<<STOP>$logf 2>&1
LOGON 10.200.100.194/dbc,dbc;

DATABASE qastage;
DROP TABLE dim_product_t2_err3;
DROP TABLE dim_product_t2_err4;

DELETE FROM dim_product_t2;

BEGIN LOADING dim_product_t2
ERRORFILES dim_product_t2_err3, dim_product_t2_err4
CHECKPOINT 100000;

SET RECORD VARTEXT ",";

DEFINE
product_key (VARCHAR(30)),
product_id (VARCHAR(30)),
product_name (VARCHAR(30)),
description (VARCHAR(30)),
FILE=$inputf;

INSERT INTO qastage.dim_product_t2(
product_key,
product_id,
product_name,
description
)
VALUES (:product_key,
:product_id,
:product_name,
:description
);
END LOADING;
LOGOFF;
EXIT
STOP
#------------------end of here doc--------------------#
RETCODE=$?
if [ $RETCODE != 0 ]
then 
echo "Eroor in loading file"
exit 1
else 
echo "File $inputf loaded successfully"
fi
#--------------end of fastload wrapper script---------#
