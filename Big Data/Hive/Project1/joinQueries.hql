use ${DB};


INSERT OVERWRITE LOCAL DIRECTORY '/tmp/inner_join' 
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY ','
select al.agent, al.login_date, al.login_time, al.logout_time, al.duration, 
ap.date ap_date, ap.total_chats, ap.avg_response_time, ap.avg_resolution_time, 
ap.avg_rating, ap.total_feedback 
from agent_login al inner join agent_performance ap 
on (al.agent = ap.agent);  


INSERT OVERWRITE LOCAL DIRECTORY '/tmp/left_join' 
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY ','
select al.agent, al.login_date, al.login_time, al.logout_time, al.duration, 
ap.date ap_date, ap.total_chats, ap.avg_response_time, ap.avg_resolution_time, 
ap.avg_rating, ap.total_feedback 
from agent_login al left join agent_performance ap 
on (al.agent = ap.agent);  


INSERT OVERWRITE LOCAL DIRECTORY '/tmp/right_join' 
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY ','
select al.agent, al.login_date, al.login_time, al.logout_time, al.duration, 
ap.date ap_date, ap.total_chats, ap.avg_response_time, ap.avg_resolution_time, 
ap.avg_rating, ap.total_feedback 
from agent_login al right join agent_performance ap 
on (al.agent = ap.agent);
