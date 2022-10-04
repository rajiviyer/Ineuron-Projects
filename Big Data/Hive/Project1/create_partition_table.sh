#!/bin/bash
TEST_DIR=`pwd`
. $TEST_DIR/config.txt

hql_file=${LOCAL_DIR}/create_partition_table.hql
beeline -u ${OPTIONS} --hivevar DB=${DB} -f ${hql_file}
