# Steps to Run Scripts
* Move data csv file from local to HDFS:  **sh moveData.sh**
* Create Hive tables & Load data: **sh ingestToHive.sh**
* Assignment Solution: **sh analysis.sh**

### Configuration (Any change in the following requires changes in the scripts too)
* Script directory: **/home/cloudera/sales_order_data**
* HDFS directory: **/tmp/sales_order_data**
* Data File (csv) Name: **sales_order_data.csv**
