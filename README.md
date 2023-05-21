# Multinational Retail Data Centralisation (Data Cleaning and Implementation)

[![made-with-python](https://img.shields.io/badge/Made%20with-Python-1f425f.svg)](https://www.python.org/)

## Overview 
> - This project looks to implement good practise of data extraction, cleaning and querying to subsequently assist in making business decisions for an example real world environment.
> - The goal will be to produce a system that extracts and stores the current company data in a database so that it's accessed from one centralised location and acts as a single source of truth for sales data. This is followed by querying the database to get up-to-date metrics for the business.

## Learning Objectives
> - To implement data extraction from a range of endpoints utilising AWS RDS databases, PDF conversions, API extractions and S3 buckets containg CSV files.
> - To clean the date from each source using pandas and postgresql, and set up primary-foreign key relations between tables.
> - To query the created database and draw out answers to real world questions.

## Project Structure

### Milestone 1 - "Extract and clean the data from the data sources."
> - Extract and clean the data from each source
> - Transfer the data over to a postgresql environment (pgAdmin 4)

### Milestone 2 - "Create the database schema."
> - Ensure proper typing of all tables and key constraints between them
> - Removing data that is not useful in drawing business conclusions

### Milestone 3 - "Querying the data"
> - Query the data to answer questions that provide insight into the business' practises and returns
> - Draw implementable conclusions

## M1 - Extract and clean the data from the data sources

### Set up the database

To initialise the database, I use pgAdmin 4 and its easy functionality tying into VSCode for this project. The Database, ```sales_data```, will serve as the blank canvas that extracted data will be imported to. 

![InitialiseDatabase](/images/setting_up_database.png)

### Initialise the 3 project Classes

To extract, clean and upload the data we will be using 3 different Classes. ```data_extraction.py``` will contain methods that help extract from each data source (CSV files, an API and an S3 bucket) under a class ```DataExtractor```. ```database_utils.py``` will connect and upload data to the pgAdmin database using Class ```DataExtractor```. ```data_cleaning.py``` will contain methods to clean each data extraction before transfering the data using Class ```DataCleaning ```.

![ThreeClasses](/images/three_classes.png)

### Extract and clean the user data

The user data is stored within an AWS RDS Database in the cloud. Credentials for the host, password, user, database and port are contained within the ```db_creds.yaml``` file which are used to access and extract from AWS. 


By reading the data into our DataCleaning class we can keep the code neat.

```python
with open('db_creds.yaml', 'r') as file:
    self.data = yaml.load(file, Loader=yaml.FullLoader)
```
Creating an instance of this class and assigning the credentials will make them callable as dictionary items.
```python
def init_db_engine(self):
    DATABASE_TYPE = 'postgresql'
    DBAPI = 'psycopg2'
    HOST = self.data['RDS_HOST']
    USER = self.data['RDS_USER']
    PASSWORD = self.data['RDS_PASSWORD']
    DATABASE = self.data['RDS_DATABASE']
    PORT = self.data['RDS_PORT']
    self.engine = create_engine(f"{DATABASE_TYPE}+{DBAPI}://{USER}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}")
```
The create_engine function is ```sqlalchemy``` functionality which must be imported prior. As this will return the endpoint which contains multiple tables, we will need to find the exact table we wish to use. To get around this issue, the method below uses the ```inspect``` function from ```sqlalchemy``` which will take in the engine above and return the table names.

```python
def list_db_tables(self):
    inspector = inspect(self.engine)
    self.tables = inspector.get_table_names() 
    return self.tables
```

In the DataExtractor class, the ```read_rds_table``` method will take the returned engine, table names and the table we wish to extract and in turn return a pandas Dataframe which we can proceed with.

```python
def read_rds_table(instance, table_name):
    instance.read_db_creds()
    engine = instance.init_db_engine()
    tables = instance.list_db_tables()
    if table_name in tables:
        users = pd.read_sql_table(table_name, engine)
        return users
    else:
        print("Invalid Table Name")
```

The database is now available and ready to be cleaned. This is done with the method ```clean_user_data``` in the DataCleaning Class. Null values, errors with dates and incorrectly typed values/rows are all removed using the below function.

```python
def clean_user_data(users):
    users['first_name'] = users['first_name'].astype('string')
    users['last_name'] = users['last_name'].astype('string')
    users['company'] = users['company'].astype('string')
    users['email_address'] = users['email_address'].astype('string')
    users['address'] = users['address'].astype('string')
    users['country'] = users['country'].astype('category')
    users['country_code'] = users['country_code'].astype('category')
    users['phone_number'] = users['phone_number'].astype('string')
    users['user_uuid'] = users['user_uuid'].astype('string')
        
    users = users.set_index('index')
        
    users = users[users['country_code'].str.len() < 4]
    users['country_code'] = users['country_code'].replace('GGB', 'GB')
        
    def standardise_phone_number(phone_number):
        phone_number = phone_number.replace(" ", "")
        phone_number = phone_number.replace("(", "")
        phone_number = phone_number.replace(")", "")
        phone_number = phone_number.replace(".", "")
        phone_number = phone_number.replace("-", "")
        phone_number = phone_number.replace("x", "")
        phone_number = list(phone_number)
        if phone_number[0] == '+':
            phone_number = phone_number[3:]
        phone_number = ''.join(phone_number)
        return phone_number

    users['phone_number'] = users['phone_number'].apply(standardise_phone_number)
    users['phone_number'] = users['phone_number'].astype('string')
        
    def standardise_date(date):
    
        if len(date) == 10:
            return date
    
        months_dict = {'January':'01', 'February':'02', 'March':'03', 'April':'04', 
                    'May':'05', 'June':'06', 'July':'07', 'August':'08',
                    'September':'09', 'October':'10', 'November':'11', 'December':'12'}
    
        split_date = date.split(" ")
        day = split_date[-1]
        if split_date[0][0].isalpha():
            month = split_date[0]
            year = split_date[1]
        else:
            month = split_date[1]
            year = split_date[0]
        month = months_dict[month]
        return f"{year}-{month}-{day}"
        
    users['date_of_birth'] = users['date_of_birth'].apply(standardise_date)
    users["date_of_birth"] = pd.to_datetime(users["date_of_birth"], format="%Y-%m-%d")
    users['join_date'] = users['join_date'].apply(standardise_date)
    users["join_date"] = pd.to_datetime(users["join_date"], format="%Y-%m-%d")
    users = users[users["join_date"] > users["date_of_birth"]]
        
    return users
```

Once cleaned it is now transfered using the ```upload_to_db``` method in the DataConnector Class.

```python
def upload_to_db(input_database, table_name):
    con = create_engine(f"postgresql+psycopg2://postgres:password@localhost:5432/sales_data")
    input_database.to_sql(table_name, con=con, if_exists='replace')
```

### Extract and clean the card details

### Extract and clean the deatils of each store

### Extract and clean the product details

### Retrieve and clean the orders table

### Retrieve and clean the data events data



## M2 - Create the database schema

### Case the columns to the correct types

### Create primary key relations

### Create foreign key constraints



## M3 - Querying the data

### Task 1: How many stores does the business have and in which countries?

### Task 2: Which locations currently have the most stores?

### Task 3: Which months product the average highest cost of sales typically?

### Task 4: How many sales are coming from online?

### Task 5: What percentage of sales come through each type of store?

### Task 6: Which month in each year product the highest cost of sales?

### Task 7: What is our staff headcount?

### Task 8: Which German store type is selling the most?

### How quickly is the company making sales?