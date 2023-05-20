-- these are notes for my own sake to recall how I cleaned the data within sql

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




