SELECT * FROM orders_table

-- ALTER TABLE orders_table
--     ALTER COLUMN user_uuid TYPE uuid USING user_uuid ::uuid
--     -- ALTER COLUMN product_quantity TYPE SMALLINT
--     -- ALTER COLUMN card_number TYPE varchar(20)

-- ALTER TABLE dim_users
--     ALTER COLUMN user_uuid TYPE uuid USING user_uuid ::uuid

SELECT * FROM dim_store_details

-- ALTER TABLE dim_store_details
--     ALTER COLUMN latitude TYPE float

SELECT * FROM dim_products

-- ALTER TABLE dim_products
-- add COLUMN weight_class varchar(14)

-- update dim_products
-- set weight_class = 'Light'
-- where weight < 2
-- set weight_class = 'Mid_Sized'
-- where weight >= 2 and weight < 40
-- set weight_class = 'Heavy'
-- where weight >= 40 and weight < 140
-- set weight_class = 'Truck_Required'
-- where weight >= 140

-- ALTER TABLE dim_products
--     ALTER COLUMN uuid TYPE uuid USING uuid ::uuid

-- ALTER TABLE dim_products
--     RENAME removed TO still_available;

-- UPDATE 
--    dim_products
-- SET 
--    still_available = REPLACE(still_available,'Still_avaliable','t')
--    still_available = REPLACE(still_available,'Removed','f')

-- ALTER TABLE dim_products
--     ALTER COLUMN still_available TYPE boolean USING still_available ::boolean

SELECT * FROM dim_date_times

-- ALTER TABLE dim_date_times
--     ALTER COLUMN date_uuid TYPE uuid USING date_uuid ::uuid

SELECT * from dim_card_details

-- ALTER TABLE dim_card_details
--     ALTER COLUMN expiry_date TYPE varchar(10)

SELECT * from orders_table

-- ALTER TABLE orders_table
-- add foreign key (date_uuid) references
-- dim_date_times(date_uuid)

-- ALTER TABLE orders_table
-- add foreign key (user_uuid) references
-- dim_users(user_uuid)

-- ALTER TABLE orders_table
-- add foreign key (card_number) references
-- dim_card_details(card_number)

-- ALTER TABLE orders_table
-- add foreign key (store_code) references
-- dim_store_details(store_code)

-- ALTER TABLE orders_table
-- add foreign key (product_code) references
-- dim_products(product_code)

SELECT * FROM dim_users

-- SELECT *
-- FROM orders_table
-- WHERE user_uuid NOT IN (SELECT user_uuid FROM dim_users);

-- Delete
-- FROM orders_table
-- WHERE user_uuid NOT IN (SELECT user_uuid FROM dim_users);

-- SELECT *
-- FROM orders_table
-- WHERE card_number NOT IN (SELECT card_number FROM dim_card_details);

-- Delete
-- FROM orders_table
-- WHERE card_number NOT IN (SELECT card_number FROM dim_card_details);

SELECT * FROM orders_table







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


