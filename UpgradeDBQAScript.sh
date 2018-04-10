#!/bin/bash
JAVA=$(which java)
MYSQL=$(which mysql)
export CATALINA_HOME=/opt/myqa2
fromdb=$1
todb=$2
sqlfile=$3
mysqluser=$4
mysqlpw=$5
echo "SET @fromdb='${1}';"
echo "SET  @todb='${2}';"
CNT=`sed -e 's/\@fromdb/'"${fromdb}"'/;s/\@todb/'"${todb}"'/' $sqlfile|$MYSQL -h "localhost" -u "$mysqluser" -p"$mysqlpw" -ss`
	if test $CNT -eq 0
	then
	echo "Minus query difference is ZERO as expected, so db test passed"
	exit $CNT
	elif test $CNT -gt 0
	then
	echo "Minus query difference is NON zero, so db test failed"
	exit $CNT
	else
	echo "Some error ocurred"
	fi
#sleep 2m
echo "test sql query executed"
