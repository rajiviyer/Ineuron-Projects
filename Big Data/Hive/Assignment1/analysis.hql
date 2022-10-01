---check rowcount in the sales_order_orc table;
set hive.cli.print.header=true
set mapreduce.job.reduces=3;

!sh echo ******Total records******
select count(*) as no_of_rows from sales_order_orc;

--Calculate total sales per year
!sh echo ******Total sales per year******
select year_id,round(sum(sales),2) total_sales from sales_order_orc group by year_id order by year_id;

--Find a product for which maximum orders were placed
!sh echo ******Product for which maximum orders were placed******
select productline, productcode,count(ordernumber) no_of_orders from sales_order_orc group by productline, productcode
order by 3 desc limit 1;


--Calculate the total sales for each quarter
!sh echo ******Total sales for each quarter******
select QTR_ID,round(sum(sales),2) total_sales from sales_order_orc group by QTR_ID order by QTR_ID;


--In which quarter sales was minimum
!sh echo ******Quarter for which sales were minimum******
select qtr_id, round(sum(sales),2) as total_sales from sales_order_orc group by qtr_id
distribute by qtr_id sort by total_sales desc limit 1;

--In which country sales was maximum and in which country sales was minimum
!sh echo ******Countries with maximum & minimum sales******
with min_max_sales as
(select min(sales) min_sales, max(sales) max_sales from sales_order_orc)
select 'Country with Maximum Sales' comment, country, sales from sales_order_orc a , min_max_sales b where a.sales = b.max_sales
union all
select 'Country with Minimum Sales' comment, country, sales from sales_order_orc a , min_max_sales b where a.sales = b.min_sales;

--Calculate quarterly sales for each city
!sh echo ******Quarterly Sales for each city******
select city, qtr_id, round(sum(sales),2) as total_sales from sales_order_orc
group by city,qtr_id
cluster by city,qtr_id;

--Find a month for each year in which maximum number of quantities were sold
!sh echo ******Month in each year where maximum quantities were sold******
with year_month_orders as
(
select year_id, month_id, sum(quantityordered) as total_orders
from sales_order_orc
group by year_id, month_id
),
rank_orders as
(
select year_id, month_id, total_orders as max_orders, rank() over (partition by year_id order by total_orders desc) as rk
from year_month_orders
)
select year_id, month_id, max_orders from rank_orders where rk = 1;

