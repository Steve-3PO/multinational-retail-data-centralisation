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

```
with open('db_creds.yaml', 'r') as file:
    self.data = yaml.load(file, Loader=yaml.FullLoader)
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