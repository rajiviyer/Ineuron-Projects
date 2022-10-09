--create database if it does not exist;
create database if not exists ${DB};
use ${DB};

--create external table to hold csv data
drop table if exists parking_violations_csv;
create external table parking_violations_csv
(
summons_number bigint,
plate_id string,
registration_state string,
plate_type string,
issue_date timestamp,
violation_code int,
vehicle_body_type string,
vehicle_make string,
issuing_agency string,
street_code1 int,
street_code2 int,
street_code3 int,
vehicle_expiration_date int,
violation_location int,
violation_precinct int,
issuer_precinct int,
issuer_code int,
issuer_command string,
issuer_squad string,
violation_time string,
time_first_observed string,
violation_county string,
violation_in_front_or_opposite string,
house_number string,
street_name string,
intersecting_street string,
date_first_observed date,
law_section int,
sub_division string,
violation_legal_code string,
days_parking_in_effect string,
from_hours_in_effect string,
to_hours_in_effect string,
vehicle_color string,
unregistered_vehicle int,
vehicle_year int,
meter_number string,
feet_from_curb int,
violation_post_code string,
violation_description string,
no_standing_or_stopping_violation string,
hydrant_violation string,
double_parking_violation string
)
row format serde'org.apache.hadoop.hive.serde2.OpenCSVSerde'
with serdeproperties (
"separatorChar" = ",",
"quoteChar" = "\"",
"timestamp.formats" = "MM/dd/yyyy"
)
stored as textfile
location '${HADOOP_MAIN_DIR}/parking_violations'
tblproperties ("skip.header.line.count" = "1");

--create ORC table to hold data in optimized way
drop table if exists parking_violations;
create table parking_violations
(
summons_number bigint,
plate_id string,
registration_state string,
plate_type string,
issue_date date,
violation_code int,
vehicle_body_type string,
vehicle_make string,
issuing_agency string,
street_code1 int,
street_code2 int,
street_code3 int,
vehicle_expiration_date int,
violation_location int,
violation_precinct int,
issuer_precinct int,
issuer_code int,
issuer_command string,
issuer_squad string,
violation_time string,
time_first_observed string,
violation_county string,
violation_in_front_or_opposite string,
house_number string,
street_name string,
intersecting_street string,
date_first_observed date,
law_section int,
sub_division string,
violation_legal_code string,
days_parking_in_effect string,
from_hours_in_effect string,
to_hours_in_effect string,
vehicle_color string,
unregistered_vehicle int,
vehicle_year int,
meter_number string,
feet_from_curb int,
violation_post_code string,
violation_description string,
no_standing_or_stopping_violation string,
hydrant_violation string,
double_parking_violation string
)
stored as ORC;

-- Copy the CSV table to the ORC table
insert overwrite table parking_violations
select 
summons_number,
plate_id,
registration_state,
plate_type,
to_date(from_unixtime(unix_timestamp(issue_date,"MM/dd/yyyy"))),
violation_code,
vehicle_body_type,
vehicle_make,
issuing_agency,
street_code1,
street_code2,
street_code3,
vehicle_expiration_date,
violation_location,
violation_precinct,
issuer_precinct,
issuer_code,
issuer_command,
issuer_squad,
case when substring(violation_time,-1) = 'P' and substring(violation_time,1,2) != '12' then concat(cast(cast(substring(violation_time,1,2) as int)+12 as string),':',substring(violation_time,3,2),':00') else concat(substring(violation_time,1,2),':',substring(violation_time,3,2),':00') end,
time_first_observed,
violation_county,
violation_in_front_or_opposite,
house_number,
street_name,
intersecting_street,
date_first_observed date,
law_section,
sub_division,
violation_legal_code,
days_parking_in_effect,
from_hours_in_effect,
to_hours_in_effect,
vehicle_color,
unregistered_vehicle,
vehicle_year,
meter_number,
feet_from_curb,
violation_post_code,
violation_description,
no_standing_or_stopping_violation,
hydrant_violation,
double_parking_violation
from parking_violations_csv
where year(from_unixtime(unix_timestamp(issue_date,"MM/dd/yyyy"))) = 2017
;
