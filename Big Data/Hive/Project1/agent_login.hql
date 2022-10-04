--create database if it does not exist;
use ${DB};

--create external table to hold csv data
drop table if exists agent_login_csv;
create external table agent_login_csv
(
SL_NO int,
AGENT string,
LOGIN_DATE date,
LOGIN_TIME string,
LOGOUT_TIME string,
DURATION string
)
row format serde'org.apache.hadoop.hive.serde2.OpenCSVSerde'
with serdeproperties (
"separatorChar" = ",",
"quoteChar" = "\"",
"timestamp.formats" = "dd-Mon-yy"
)
stored as textfile
location '${HADOOP_MAIN_DIR}/agent_login'
tblproperties ("skip.header.line.count" = "1");

--create ORC table to hold data in optimized way
drop table if exists agent_login;
create table agent_login
(
SL_NO int,
AGENT string,
LOGIN_DATE date,
LOGIN_TIME timestamp,
LOGOUT_TIME timestamp,
DURATION int
)
stored as ORC;

-- Copy the CSV table to the ORC table
insert overwrite table agent_login
select 
sl_no,
agent,
login_date,
from_unixtime(login_time),
from_unixtime(logout_time),
--logout_time - login_time
duration
from
(
select 
sl_no, 
trim(agent) as agent,
to_date(from_unixtime(unix_timestamp(login_date,"dd-MMM-yy"))) as login_date,
unix_timestamp(concat(login_date,' ',login_time),"dd-MMM-yy hh:mm:ss") as login_time,
unix_timestamp(concat(login_date,' ',logout_time),"dd-MMM-yy hh:mm:ss") as logout_time,
hour(duration)*3600 + minute(duration)*60 + second(duration) as duration
from agent_login_csv
) as t
;
