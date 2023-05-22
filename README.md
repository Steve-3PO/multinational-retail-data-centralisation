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

The raw form of the card details data is a PDF file. The package ```tabula.py``` can be used to extract this data in its raw form and convert it directly to a pandas Dataframe.

As tabula returns a list of dictionaries from the ```read_pdf``` function, the list must be concatinated before we can return a dataframe.

```python
def retrieve_pdf_data(link):
    list_of_dbs = tabula.read_pdf(link, pages='all')
    df = pd.concat(list_of_dbs, ignore_index=True)
    return df
```

Once returned we can proceed to clean the card data. This is done in the ```clean_card_data``` method within the DataCleaning Class. As the datacleaning follows similar procedures to the user data, the code I have decided to not list however it is there to read in the method as mentioned if you wish to see exactly.

The data is then uploaded again using the ```upload_to_db``` method. 

### Extract and clean the details of each store

The data for the stores are retried through the use of an API. To connect to the API, the API key must be included in the retrieval and is contained in the ```api_key.yaml``` file. The API has 2 GET methods, one which will return the number of stores, and the other will retrieve the data for each of them. The latter endpoint is of the form ```https://.../{store_number}```. 

The method ```list_number_of_stores``` within the DataExtractor Class retrieves the number of stores that exist from the endpoints. Using the ```requests``` library, we can access the endpoint as shown below.

```python
def list_number_of_stores(no_of_stores_endpoint, header_dict):
    response = requests.get(no_of_stores_endpoint, headers=header_dict)
    num = response.json()['number_stores']
    return num
```

Once the total store number number is known, a for loop is used to individually access each store endpoint and append together with an empty list. As this will create a list of dictionaries, the ```.DataFrame``` function is used to convert to one global Dataframe. It is important to note that the ```header_dict``` is the credentials from the yaml file extracted to a dictionary variable.

```python
def retrieve_stores_data(retrieve_a_store_endpoint, header_dict):
    lst2 = []
    for r in range(num):
        curr_string = retrieve_a_store_endpoint + f'{r}'
        response = requests.get(curr_string, headers=header_dict)
        output = response.json()
        lst2.append(output)
    return pd.DataFrame(lst2)
```

Now the dataframe is obtained, it is cleaned and uploaded in the same procedure as defined previously.

### Extract and clean the product details

The product data is stored in CSV format in an S3 bucket on AWS. To extract this data, the ```boto3``` package must be installed and imported prior. As the url provided is ```s3://data-handling-public/products.csv``` we must separate this to utilise the ```download_file``` functionality. 

```python
def extract_from_s3(url_address):
    s3 = boto3.client('s3')
    url_address = url_address.split('/')
    s3.download_file(url_address[-2], url_address[-1], 's3_product_data.csv')
    uncleaned_product_data = pd.read_csv('s3_product_data.csv')
    return uncleaned_product_data
```

Fortunately, the ```download_file``` function will provide a CSV file which can be directly converted to a DataFrame with ```read_csv```. Allowing it to be cleaned and transfered over the pgadmin in the same methods defined above for the user data.

It is important to note that an extra step of converting product weights was used here. The method ```convert_product_weights``` is listed below which takes the value, finds the unit multiplier and removes and unnecessary errors in the listing of the weight such as special characters.

```python
def standardise_weight(weight):
    if weight == 'nan':
        return weight
    if 'kg' in weight:
        multiplier = 1
    elif 'k' not in weight:
        if 'oz' not in weight:
            multiplier = 1000
        else:
            multiplier = 35.274
    output = ''
    for letter in weight:
        if letter.isnumeric() or letter == '.':
            output += letter
        elif letter == 'x':
            multiplier = multiplier / float(output)
            output = ''
        elif letter.isalpha():
            return round(float(output)/multiplier, 3)
```

### Retrieve and clean the orders table

The orders table which acts as the single source of truth for all orders the company has made in the past is stored in a database on AWS RDS. Fortunately, reusing the ```list_db_tables``` and ```read_rds_table``` will return the Dataframe ready for cleaning and importing again with the ```upload_to_db``` method.

### Retrieve and clean the data events data

The final source of data is a JSON file containing the details of when each sale happened, as well as related attributes. The file is stored on S3 and can be found at the following link ```https://data-handling-public.s3.eu-west-1.amazonaws.com/date_details.json```. As this link is intact and usable instantly it is possible to just use the ```read_json``` function to return the unclean DataFrame.

## M2 - Create the database schema

### Case the columns to the correct types

Although cleaning involved type casting the data correctly, often this can lead to errors if it is not taken across to pgadmin correctly. The data for these tables must be correctly typed so that we can create key constraints and call the data accurately. The types are viewable in pgadmin however it is important to note that you cannot change types to just anything. Only types that are related to the current type of the data column are provided. This means that changes must be made in using SQL queries first.

![type_casting](/images/type_casting.png)

UUID's of all tables must be converted to the UUID data type, which is not a string to clarify. The example below casts the user_uuid within the orders table to the UUID data type.

```sql
ALTER TABLE orders_table
ALTER COLUMN user_uuid TYPE uuid USING user_uuid ::uuid
```

Card numbers, names, addresses must also be cast correctly. Varchars are used to limit the input taken from the user and provide a localised constraint on the column.

```sql
ALTER TABLE orders_table
ALTER COLUMN card_number TYPE varchar(20)
```

Quantities can be kept in any form of integer however I choose to represent these as ```SMALLINT``` given that quantities of stock generally don't get too large.

```sql
ALTER TABLE orders_table
ALTER COLUMN product_quantity TYPE SMALLINT
```

Dates must be typed correctly otherwise you cannot utilise time dependent queries as SQL will not understand the comparisons being made (date1 < date2). Fortunately during cleaning I chose to type cast columns to ```datetime64``` with pandas and this means SQL will provide the correct assumption that these are dates, subsequently providing the associated date types in pgadmin, such as ```date```

Along with type casting, additional columns were added to provide more insight into the data and help with categorisation. An example of this is providing a ```weight_class``` column in the products table that will divide the data into buckets for weights between 2:40:140:140+. 

```sql
ALTER TABLE dim_products
add COLUMN weight_class varchar(14)

update dim_products
set weight_class = 'Light'
where weight < 2
```

### Create primary key relations

Now that the tables have the appropriate data types, the primary keys to each of the tables prefixed with dim are added. Each table will serve the orders_table which will be the single source of truth for the orders. The can be done in SQL using the code below which adds a primary key constraint in ```table``` on the data ```variable```.

```sql
ALTER TABLE table
ADD PRIMARY KEY (variable);
```

However it is also possible to use pgadmin to simply change these with its ```primary key?``` toggle indicated in the screenshot shown.

![primary_key_relation](/images/primary_key_relation.png)

### Create foreign key constraints

The issue with linking foreign keys to primary keys is that if the data is not matched an error will be thrown. In connecting the orders table to each of the 5 primary keys, 3 worked as was the case for ```product_code``` for example which returned the constraint correctly implemented.

```sql
ALTER TABLE orders_table
add foreign key (product_code) references
dim_products(product_code)
```

Whilst 2 others returned an error. This occurs because there are entries within the orders table that do not appear in the parent table with the primary key. To fix this, it is okay to simply remove those entries as without links to the other tables, these rows are not useful.

```sql
DELETE *
FROM orders_table
WHERE user_uuid NOT IN (SELECT user_uuid FROM dim_users);

ALTER TABLE orders_table
add foreign key (user_uuid) references
dim_users(user_uuid)
```

## M3 - Querying the data

The set of tasks listed below provide insight into the business' return and statistics.         

### Task 1: How many stores does the business have and in which countries?

```sql
SELECT country_code as country, count(*) from dim_store_details
GROUP BY country_code
```

![task_1](/images/task_1.png)

### Task 2: Which locations currently have the most stores?

```sql
SELECT locality, count(*) as total_no_stores
from dim_store_details
GROUP BY locality
order by count(*) desc
limit 7
```
![task_2](/images/task_2.png)

### Task 3: Which months product the average highest cost of sales typically?

```sql
select sum(dim_products.product_price * product_quantity) as total_sales, 
dim_date_times.month
from orders_table
join dim_date_times on orders_table.date_uuid = dim_date_times.date_uuid
join dim_products on orders_table.product_code = dim_products.product_code
GROUP by dim_date_times.month
order by sum(dim_products.product_price * product_quantity) desc
limit 6
```
![task_3](/images/task_3.png)

### Task 4: How many sales are coming from online?

```sql
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
```
![task_4](/images/task_4.png)

### Task 5: What percentage of sales come through each type of store?

```sql
select store_type, sum(product_quantity * product_price) as total_sales,
(sum(product_quantity * product_price) * 100 )/SUM(sum(product_quantity * product_price)) over () as Percentage_of_Total
from orders_table
join dim_store_details 
on orders_table.store_code = dim_store_details.store_code
join dim_products
on orders_table.product_code = dim_products.product_code
group by store_type
order by sum(product_quantity * product_price) desc
```
![task_5](/images/task_5.png)

### Task 6: Which month in each year product the highest cost of sales?

```sql
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
```
![task_6](/images/task_6.png)

### Task 7: What is our staff headcount?

```sql
select sum(staff_numbers) as total_staff_numbers, country_code 
from dim_store_details
group by country_code
order BY sum(staff_numbers) desc
```
![task_7](/images/task_7.png)

### Task 8: Which German store type is selling the most?

```sql
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
```
![task_8](/images/task_8.png)

### Task 9: How quickly is the company making sales?

```sql
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
```
![task_9](/images/task_9.png)