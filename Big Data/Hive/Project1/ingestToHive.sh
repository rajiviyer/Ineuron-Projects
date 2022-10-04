#!/bin/bash
TEST_DIR=`pwd`
. $TEST_DIR/config.txt

declare -a hql_files=("${LOCAL_DIR}/agent_login.hql" "${LOCAL_DIR}/agent_performance.hql")
for val in "${hql_files[@]}"; 
do
  beeline -u ${OPTIONS} --hivevar DB=${DB} --hivevar "HADOOP_MAIN_DIR=${HADOOP_MAIN_DIR}" -f $val
done
