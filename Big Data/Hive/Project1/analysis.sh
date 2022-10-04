#!/bin/bash
TEST_DIR=`pwd`
. $TEST_DIR/config.txt

hql_file=${LOCAL_DIR}/analysis.hql
beeline --headerInterval=1000 --silent=true --hivevar DB=${DB} -u ${OPTIONS} -f ${hql_file}
