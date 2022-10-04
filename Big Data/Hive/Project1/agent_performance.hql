--create database if it does not exist;
use ${DB};

--create external table to hold csv data
drop table if exists agent_performance_csv;
create external table agent_performance_csv
(
SL_NO int,
DATE date,
AGENT string,
TOTAL_CHATS int,
AVG_RESPONSE_TIME string,
AVG_RESOLUTION_TIME string,
AVG_RATING double,
TOTAL_FEEDBACK int
)
row format serde'org.apache.hadoop.hive.serde2.OpenCSVSerde'
with serdeproperties (
"separatorChar" = ",",
"quoteChar" = "\"",
"timestamp.formats" = "MM/dd/yyyy"
)
stored as textfile
location '${HADOOP_MAIN_DIR}/agent_performance'
tblproperties ("skip.header.line.count" = "1");

--create ORC table to hold data
drop table if exists agent_performance;
create table agent_performance
(
SL_NO int,
DATE date,
AGENT string,
TOTAL_CHATS int,
AVG_RESPONSE_TIME int,
AVG_RESOLUTION_TIME int,
AVG_RATING double,
TOTAL_FEEDBACK int
)
stored as ORC;

-- Copy the CSV table to the ORC table
insert overwrite table agent_performance
select 
sl_no,
to_date(from_unixtime(unix_timestamp(date,"MM/dd/yyyy"))),
trim(agent),
total_chats,
hour(avg_response_time)*3600 + minute(avg_response_time)*60 + second(avg_response_time),
hour(avg_resolution_time)*3600 + minute(avg_resolution_time)*60 + second(avg_resolution_time),
avg_rating,
total_feedback
from
agent_performance_csv
;
