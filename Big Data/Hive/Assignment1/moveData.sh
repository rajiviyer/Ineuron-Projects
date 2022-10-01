#!/bin/bash

local_dir=/home/cloudera/sales_order_data
hadoop_main_dir=/tmp/sales_order_data
filename=sales_order_data.csv

hadoop fs -mkdir -p ${hadoop_main_dir}
echo "Hadoop directory ${hadoop_main_dir} created" 

hadoop fs -rm ${hadoop_main_dir}/${filename}
echo "Remove any existing File ${filename} in Hadoop directory"

hadoop fs -put ${local_dir}/${filename} ${hadoop_main_dir}
echo "File ${filename} moved to Hadoop directory"
