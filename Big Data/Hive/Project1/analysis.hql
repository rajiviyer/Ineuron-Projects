use ${DB};

set hive.cli.print.header=true
set mapreduce.job.reduces=3;

!sh echo ******List all existing agents******
select distinct agent from
(
select agent from agent_login 
union all 
select agent from agent_performance
) as t;

!sh echo ******Find average rating for each agent******
select agent, 
round(avg(avg_rating),2) as avg_rating 
from agent_performance 
group by agent 
distribute by agent;

!sh echo ******Find total days worked by each agent******
select agent, 
count(distinct login_date) as days_count 
from agent_login group by agent
distribute by agent;

!sh echo ******Total queries supported by each agent******
--Asumming each chat is a query
select agent, 
sum(total_chats) as total_chats
from agent_performance 
group by agent 
distribute by agent;

!sh echo ******Total feedback received by each agent******
select agent, 
sum(total_feedback) as total_feedback
from agent_performance 
group by agent 
distribute by agent;

!sh echo ******Agents who have average rating between 3.5 to 4******
select distinct agent from agent_performance where avg_rating between 3.5 and 4;

!sh echo ******Agents who have average rating less than 3.5******
select distinct agent from agent_performance where avg_rating < 3.5;

!sh echo ******Agents who have average rating more than 4.5******
select distinct agent from agent_performance where avg_rating > 4.5;

!sh echo ******Total Feedback of Agents who have got average rating more than 4.5******
select a.agent,
sum(a.total_feedback) as total_feedback 
from agent_performance a
where a.agent in (select agent from agent_performance where avg_rating > 4.5)
group by a.agent
distribute by a.agent
sort by total_feedback desc;


!sh echo ******Average weekly response time (in seconds) for each agent******
select agent, 
round(avg(case when date_format(date,'W') == 1 then avg_response_time end),2) as week1,
round(avg(case when date_format(date,'W') == 2 then avg_response_time end),2) as week2,
round(avg(case when date_format(date,'W') == 3 then avg_response_time end),2) as week3,
round(avg(case when date_format(date,'W') == 4 then avg_response_time end),2) as week4,
round(avg(case when date_format(date,'W') == 5 then avg_response_time end),2) as week5
from agent_performance
group by agent
distribute by agent;


!sh echo ******Average weekly resolution time (in seconds) for each agent******
select agent, 
round(avg(case when date_format(date,'W') == 1 then avg_resolution_time end),2) as week1,
round(avg(case when date_format(date,'W') == 2 then avg_resolution_time end),2) as week2,
round(avg(case when date_format(date,'W') == 3 then avg_resolution_time end),2) as week3,
round(avg(case when date_format(date,'W') == 4 then avg_resolution_time end),2) as week4,
round(avg(case when date_format(date,'W') == 5 then avg_resolution_time end),2) as week5
from agent_performance
group by agent
distribute by agent;


!sh echo ******For each Agent find the number of chats for which they received a feedback******
select agent, 
sum(total_chats) as total_chats
from agent_performance
where total_feedback > 0
group by agent
distribute by agent
sort by total_chats desc;

!sh echo ******Total contribution hours for each agent weekly basis******
select agent, 
round(sum(case when date_format(login_date,'W') == 1 then duration else 0 end)/3600,2) as week1,
round(sum(case when date_format(login_date,'W') == 2 then duration else 0 end)/3600,2) as week2,
round(sum(case when date_format(login_date,'W') == 3 then duration else 0 end)/3600,2) as week3,
round(sum(case when date_format(login_date,'W') == 4 then duration else 0 end)/3600,2) as week4,
round(sum(case when date_format(login_date,'W') == 5 then duration else 0 end)/3600,2) as week5
from agent_login
group by agent
distribute by agent;

