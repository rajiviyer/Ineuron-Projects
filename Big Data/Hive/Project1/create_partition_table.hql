use ${DB};

set mapreduce.job.reduces=3;
set hive.exec.dynamic.partition.mode=nonstrict;   

DROP TABLE IF EXISTS agent_login_partitioned;

CREATE TABLE agent_login_partitioned
(
SL_NO int,
LOGIN_DATE date,
LOGIN_TIME timestamp,
LOGOUT_TIME timestamp,
DURATION int
)
PARTITIONED BY (AGENT string)
CLUSTERED BY (SL_NO) INTO 20 BUCKETS
stored as ORC;

insert overwrite table agent_login_partitioned
PARTITION(agent)
select
sl_no,
to_date(from_unixtime(unix_timestamp(login_date,"dd-MMM-yy"))) as login_date,
from_unixtime(unix_timestamp(concat(login_date,' ',login_time),"dd-MMM-yy hh:mm:ss")) as login_time,
from_unixtime(unix_timestamp(concat(login_date,' ',logout_time),"dd-MMM-yy hh:mm:ss")) as logout_time,
hour(duration)*3600 + minute(duration)*60 + second(duration) as duration,
trim(agent) as agent
from agent_login_csv
distribute by agent
;
