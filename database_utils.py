from sqlalchemy import create_engine, inspect
import pandas as pd
import yaml
import tabula

class DatabaseConnector:
    
    def __init__(self) -> None:
        pass
    
    def read_db_creds(self):
        with open('db_creds.yaml', 'r') as file:
            self.data = yaml.load(file, Loader=yaml.FullLoader)
        
    def init_db_engine(self):
        DATABASE_TYPE = 'postgresql'
        DBAPI = 'psycopg2'
        HOST = self.data['RDS_HOST']
        USER = self.data['RDS_USER']
        PASSWORD = self.data['RDS_PASSWORD']
        DATABASE = self.data['RDS_DATABASE']
        PORT = self.data['RDS_PORT']
        self.engine = create_engine(f"{DATABASE_TYPE}+{DBAPI}://{USER}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}")
        return self.engine
      
    def list_db_tables(self):
        inspector = inspect(self.engine)
        self.tables = inspector.get_table_names()
        return self.tables
    
    def upload_to_db(input_database, table_name):
        con = create_engine(f"postgresql+psycopg2://postgres:password@localhost:5432/sales_data")
        input_database.to_sql(table_name, con=con, if_exists='replace')
        
    def retrieve_pdf_data(link):
        list_of_dbs = tabula.read_pdf(link, pages='all')
        df = pd.concat(list_of_dbs, ignore_index=True)
        return df
    
    
