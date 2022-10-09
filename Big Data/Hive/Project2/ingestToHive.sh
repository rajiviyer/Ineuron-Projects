#!/bin/bash
TEST_DIR=`pwd`
. $TEST_DIR/config.txt

hql_file=${LOCAL_DIR}/parking_violations.hql
beeline -u ${OPTIONS} --hivevar DB=${DB} --hivevar "HADOOP_MAIN_DIR=${HADOOP_MAIN_DIR}" -f ${hql_file}
