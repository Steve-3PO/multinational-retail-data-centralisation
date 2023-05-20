# I used this file as the control center

import pandas as pd
import yaml
import tabula
import re
import numpy as np
import boto3
from sqlalchemy import create_engine, inspect
from database_utils import DatabaseConnector
from data_extraction import DataExtractor
from data_cleaning import DataCleaning

# db = DatabaseConnector() 
# db.read_db_creds() 
# db.init_db_engine()
# db.list_db_tables()
                
#data = DatabaseConnector.read_db_creds()
#print(data)
#engine = DatabaseConnector.init_db_engine(data)
#print(engine)
#tables = DatabaseConnector.list_db_tables(engine)


# db = DatabaseConnector() 
# users = DataExtractor.read_rds_table(db, 'legacy_users')     
# cleaned_data = DataCleaning.clean_user_data(users)
# imported = DatabaseConnector.upload_to_db(cleaned_data, 'dim_users')

# dim_card_details = DatabaseConnector.retrieve_pdf_data('https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf')
# cleaned_card_data = DataCleaning.clean_card_data(dim_card_details)
# imported_cards = DatabaseConnector.upload_to_db(cleaned_card_data, 'dim_card_details')

# with open('api_key.yaml', 'r') as file:
#     key_dict = yaml.load(file, Loader=yaml.FullLoader)
# num = DataExtractor.list_number_of_stores('https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores', key_dict)
# api_db = DataExtractor.retrieve_stores_data(f'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/', key_dict)
# api_cleaned_db = DataCleaning.called_clean_store_data(api_db)
# imported_stores = DatabaseConnector.upload_to_db(api_cleaned_db, 'dim_store_details')

# unclean_product_data = DataExtractor.extract_from_s3('s3://data-handling-public/products.csv')
# cleaned_weight_product_data = DataCleaning.convert_product_weights(unclean_product_data)
# fully_cleaned_prod_data = DataCleaning.clean_products_data(cleaned_weight_product_data)
# imported_prod_data = DatabaseConnector.upload_to_db(fully_cleaned_prod_data, 'dim_products')


# db = DatabaseConnector() 
# orders = DataExtractor.read_rds_table(db, 'orders_table') 
# cleaned_orders = DataCleaning.clean_orders_data(orders)
# imported_orders_data = DatabaseConnector.upload_to_db(cleaned_orders, 'orders_table')

# events = DataExtractor.extract_from_link('https://data-handling-public.s3.eu-west-1.amazonaws.com/date_details.json')
# cleaned_events = DataCleaning.clean_date_events_data(events)
# imported_events = DatabaseConnector.upload_to_db(cleaned_events, 'dim_date_times')