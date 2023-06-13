from sqlalchemy import create_engine, inspect
import yaml 
import pandas as pd
import tabula
import requests
import boto3

class DataExtractor:
    
    def read_rds_table(instance, table_name):
        instance.read_db_creds()
        engine = instance.init_db_engine()
        tables = instance.list_db_tables()
        if table_name in tables:
            users = pd.read_sql_table(table_name, engine)
            return users
        else:
            print("Invalid Table Name")
            
    def list_number_of_stores(no_of_stores_endpoint, header_dict):
        response = requests.get(no_of_stores_endpoint, headers=header_dict)
        num = response.json()['number_stores']
        return num
    
    def retrieve_stores_data(retrieve_a_store_endpoint, header_dict):
        list_of_stores = []
        for r in range(451):
            curr_string = retrieve_a_store_endpoint + f'{r}'
            response = requests.get(curr_string, headers=header_dict)
            output = response.json()
            list_of_stores.append(output)
            
        return pd.DataFrame(list_of_stores)
    
    def extract_from_s3(url_address):
        s3 = boto3.client('s3')
        url_address = url_address.split('/')
        s3.download_file(url_address[-2], url_address[-1], 's3_product_data.csv')
        uncleaned_product_data = pd.read_csv('s3_product_data.csv')
        return uncleaned_product_data
    
    def extract_from_link(link):
        return pd.read_json(link)
        
