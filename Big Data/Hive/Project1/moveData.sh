#!/bin/bash

TEST_DIR=`pwd`
. $TEST_DIR/config.txt

declare -a agents_data=( "agent_login" "agent_performance")
for data in "${agents_data[@]}"; 
do 
	echo ${data}
	hadoop fs -mkdir -p ${HADOOP_MAIN_DIR}/${data}
	hadoop fs -rm ${HADOOP_MAIN_DIR}/${data}/*.csv
	hadoop fs -put ${LOCAL_DIR}/${data}.csv ${HADOOP_MAIN_DIR}/${data}/
	hadoop fs -ls ${HADOOP_MAIN_DIR}/${data}/
done
