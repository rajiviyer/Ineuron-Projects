#!/bin/bash
TEST_DIR=`pwd`
. $TEST_DIR/config.txt

sudo rm -rf /tmp/inner_join
sudo rm -rf /tmp/left_join
sudo rm -rf /tmp/right_join

hql_file=${LOCAL_DIR}/joinQueries.hql
beeline -u ${OPTIONS} --hivevar DB=${DB} --hivevar LOCAL_DIR=${LOCAL_DIR} -f ${hql_file}
