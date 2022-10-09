#!/bin/bash

TEST_DIR=`pwd`
. $TEST_DIR/config.txt
data=parking_violations
echo ${data}
hadoop fs -mkdir -p ${HADOOP_MAIN_DIR}/${data}
hadoop fs -rm -f ${HADOOP_MAIN_DIR}/${data}/*.csv
hadoop fs -put ${LOCAL_DIR}/${data}.csv ${HADOOP_MAIN_DIR}/${data}/
hadoop fs -ls ${HADOOP_MAIN_DIR}/${data}/
