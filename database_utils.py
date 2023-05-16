import pandas as pd
import yaml
import tabula
from sqlalchemy import create_engine, inspect
#from data_extraction import DataExtractor
#from data_cleaning import DataCleaning

class DatabaseConnector:
    
    def __init__(self) -> None:
        pass
    
    def read_db_creds(self):
        with open('db_creds.yaml', 'r') as file:
            self.data = yaml.load(file, Loader=yaml.FullLoader)
            #print(self.data)
            return self.data
        
    def init_db_engine(self):
        DATABASE_TYPE = 'postgresql'
        DBAPI = 'psycopg2'
        HOST = self.data['RDS_HOST']
        #print(data['RDS_HOST'])
        USER = self.data['RDS_USER']
        #print(data['RDS_USER'])
        PASSWORD = self.data['RDS_PASSWORD']
        #print(data['RDS_PASSWORD'])
        DATABASE = self.data['RDS_DATABASE']
        #print(data['RDS_DATABASE'])
        PORT = self.data['RDS_PORT']
        self.engine = create_engine(f"{DATABASE_TYPE}+{DBAPI}://{USER}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}")
        return self.engine
      
    def list_db_tables(self):
        inspector = inspect(self.engine)
        self.tables = inspector.get_table_names()
        print(self.tables)  
        return self.tables
    
    def upload_to_db(input_database, table_name):
        con = create_engine(f"postgresql+psycopg2://postgres:password@localhost:5432/sales_data")
        input_database.to_sql(table_name, con=con, if_exists='replace')
        
    def retrieve_pdf_data(link):
        list_of_dbs = tabula.read_pdf(link, pages='all')
        df = pd.concat(list_of_dbs, ignore_index=True)
        #print(len(df))
        return df
    
    
#db = DatabaseConnector() 
#db.read_db_creds() 
#db.init_db_engine()
#db.list_db_tables()
                
#data = DatabaseConnector.read_db_creds()
#print(data)
#engine = DatabaseConnector.init_db_engine(data)
#print(engine)
#tables = DatabaseConnector.list_db_tables(engine)


# db = DatabaseConnector() 
# users = DataExtractor.read_rds_table(db, 'legacy_users')     
# cleaned_data = DataCleaning.clean_user_data(users)
# imported = DatabaseConnector.upload_to_db(cleaned_data, 'dim_users')

#dim_card_details = DatabaseConnector.retrieve_pdf_data('https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf')
#print(dim_card_details)