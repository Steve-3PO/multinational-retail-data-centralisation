
-- task 1 - how many stores does the business have and in which countries
SELECT country_code as country, count(*) from dim_store_details
GROUP BY country_code

-- task 2 - which locations currently have the most stores?
SELECT locality, count(*) as total_no_stores
from dim_store_details
GROUP BY locality
order by count(*) desc
limit 7

-- task 3 - which months produce the average highest cost of sales typically?

select sum(dim_products.product_price * product_quantity) as total_sales, 
dim_date_times.month
from orders_table
join dim_date_times on orders_table.date_uuid = dim_date_times.date_uuid
join dim_products on orders_table.product_code = dim_products.product_code
GROUP by dim_date_times.month
order by sum(dim_products.product_price * product_quantity) desc
limit 6

--  task 4 - how many sales are coming from online?

select sum(number_of_sales) as number_of_sales, 
sum(product_quantity_count) as product_quantity_count, 
location
from 
(
select count(*) as number_of_sales, 
sum(product_quantity) as product_quantity_count,
case dim_store_details.store_type 
when 'Web Portal' then 'Web'
else 'Offline' end as location
from orders_table
join dim_store_details on 
orders_table.store_code = dim_store_details.store_code
group by dim_store_details.store_type
) 
as derivedTable
group by location
order BY LOCATION desc

-- take 5 - what percentage of sales come through each type of store?

select store_type, sum(product_quantity * product_price) as total_sales,
(sum(product_quantity * product_price) * 100 )/SUM(sum(product_quantity * product_price)) over () as Percentage_of_Total
from orders_table
join dim_store_details 
on orders_table.store_code = dim_store_details.store_code
join dim_products
on orders_table.product_code = dim_products.product_code
group by store_type
order by sum(product_quantity * product_price) desc

-- task 6 - which month in each year product the highest cost of sales?

select sum(product_quantity * product_price) as total_sales,
year, month
from orders_table
join dim_date_times 
on orders_table.date_uuid = dim_date_times.date_uuid
join dim_products
on orders_table.product_code = dim_products.product_code
group by year, month
order by sum(product_quantity * product_price) desc
limit 10

-- task 7 - whats is our staff headcount?

select sum(staff_numbers) as total_staff_numbers, country_code 
from dim_store_details
group by country_code
order BY sum(staff_numbers) desc

-- task 8 - which german store type is selling the most?

select sum(product_price * product_quantity) as total_sales,
store_type, country_code 
from orders_table
join dim_products on
orders_table.product_code = dim_products.product_code
join dim_store_details on
orders_table.store_code = dim_store_details.store_code
where country_code = 'DE'
group by store_type, country_code
order BY sum(product_price * product_quantity)

-- task 9 - how quickly is the company making sales
select year, avg(actual_time) 
from 
(
select year,
iso - lead (iso, 1) over (order by year, month, day, timestamp) as actual_time
from dim_date_times
group by year, month, day, timestamp, iso
) 
as firstset
group by year
order by avg(actual_time) 
