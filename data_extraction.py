import yaml 
import pandas as pd
import tabula
import requests
import boto3
from sqlalchemy import create_engine, inspect
#from database_utils import DatabaseConnector

class DataExtractor:
    def read_rds_table(instance, table_name):
        instance.read_db_creds()
        engine = instance.init_db_engine()
        tables = instance.list_db_tables()
        if table_name in tables:
            users = pd.read_sql_table(table_name, engine)
            #print(users)
            return users
        else:
            print("Invalid Table Name")
    def list_number_of_stores(no_of_stores_endpoint, header_dict):
        response = requests.get(no_of_stores_endpoint, headers=header_dict)
        num = response.json()['number_stores']
        return num
    
    def retrieve_stores_data(retrieve_a_store_endpoint, header_dict):
        lst2 = []
        for r in range(451):
            curr_string = retrieve_a_store_endpoint + f'{r}'
            response = requests.get(curr_string, headers=header_dict)
            output = response.json()
            lst2.append(output)
        return pd.DataFrame(lst2)
    
    def extract_from_s3(url_address):
        s3 = boto3.client('s3')
        url_address = url_address.split('/')
        s3.download_file(url_address[-2], url_address[-1], 's3_product_data.csv')
        uncleaned_product_data = pd.read_csv('s3_product_data.csv')
        #print(uncleaned_product_data)
        return uncleaned_product_data
    
    def extract_from_link(link):
        return pd.read_json(link)
        
# db = DatabaseConnector() 
# users = DataExtractor.read_rds_table(db, 'legacy_users')


#with open('api_key.yaml', 'r') as file:
    #key_dict = yaml.load(file, Loader=yaml.FullLoader)
#num = DataExtractor.list_number_of_stores('https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores', key_dict)
#api_db = DataExtractor.retrieve_stores_data(f'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/', key_dict)
#print(api_db)