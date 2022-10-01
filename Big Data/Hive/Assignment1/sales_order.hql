--create new db sales_db and connect to it
create database if not exists sales_db;
use sales_db;

--create internal hive table sales_order_csv which stores data present in the /home/cloudera/sales_order_data/sales_order_data.csv file

drop table if exists sales_order_csv;

create table sales_order_csv
(
ORDERNUMBER int,
QUANTITYORDERED int,
PRICEEACH float,
ORDERLINENUMBER int,
SALES float,
STATUS string,
QTR_ID int,
MONTH_ID int,
YEAR_ID int,
PRODUCTLINE string,
MSRP int,
PRODUCTCODE string,
PHONE string,
CITY string,
STATE string,
POSTALCODE string,
COUNTRY string,
TERRITORY string,
CONTACTLASTNAME string,
CONTACTFIRSTNAME string,
DEALSIZE string
)
row format delimited
fields terminated by ','
tblproperties("skip.header.line.count"="1")
; 

--load sales_order data from the csv file stored in hadoop
load data inpath '/tmp/sales_order_data/sales_order_data.csv' into table sales_order_csv;

--describe table sales_order_csv
describe formatted sales_order_csv;

--create internal hive table sales_order_orc stored in orc format
drop table if exists sales_order_orc;
create table sales_order_orc
(
ORDERNUMBER int,
QUANTITYORDERED int,
PRICEEACH float,
ORDERLINENUMBER int,
SALES float,
STATUS string,
QTR_ID int,
MONTH_ID int,
YEAR_ID int,
PRODUCTLINE string,
MSRP int,
PRODUCTCODE string,
PHONE string,
CITY string,
STATE string,
POSTALCODE string,
COUNTRY string,
TERRITORY string,
CONTACTLASTNAME string,
CONTACTFIRSTNAME string,
DEALSIZE string
)
stored as orc;

-- Copy the CSV table to the ORC table
insert overwrite table sales_order_orc
select
ORDERNUMBER,
QUANTITYORDERED,
PRICEEACH,
ORDERLINENUMBER,
SALES,
STATUS,
QTR_ID,
MONTH_ID,
YEAR_ID,
PRODUCTLINE,
MSRP,
PRODUCTCODE,
PHONE,
CITY,
STATE,
POSTALCODE,
COUNTRY,
TERRITORY,
CONTACTLASTNAME,
CONTACTFIRSTNAME,
DEALSIZE
from sales_order_csv;

--check existence of the newly created tables
show tables;
