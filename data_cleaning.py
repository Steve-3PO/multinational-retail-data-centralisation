import pandas as pd
import yaml
import tabula
import re
import numpy as np
# from sqlalchemy import create_engine, inspect
# from database_utils import DatabaseConnector
# from data_extraction import DataExtractor

class DataCleaning:
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
            # print(day)
            # print(month)
            # print(year)
            return f"{year}-{month}-{day}"
        
        users['date_of_birth'] = users['date_of_birth'].apply(standardise_date)
        users["date_of_birth"] = pd.to_datetime(users["date_of_birth"], format="%Y-%m-%d")
        users['join_date'] = users['join_date'].apply(standardise_date)
        users["join_date"] = pd.to_datetime(users["join_date"], format="%Y-%m-%d")
        
        users = users[users["join_date"] > users["date_of_birth"]]
        
        return users
    
    def clean_card_data(dim_card_details):
        
        dim_card_details['card_number'] = dim_card_details['card_number'].astype('string')
        dim_card_details['card_provider'] = dim_card_details['card_provider'].astype('string')
        dim_card_details = dim_card_details[dim_card_details['expiry_date'].str.len() == 5]
        
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
            # print(day)
            # print(month)
            # print(year)
            return f"{year}-{month}-{day}"
        
        dim_card_details['date_payment_confirmed'] = dim_card_details['date_payment_confirmed'].apply(standardise_date)
        dim_card_details['date_payment_confirmed'] = pd.to_datetime(dim_card_details['date_payment_confirmed'], format="%Y-%m-%d")
        dim_card_details['expiry_date'] = pd.to_datetime(dim_card_details['expiry_date'], format="%m/%y")
        return dim_card_details
    
    def called_clean_store_data(api_db):
        api_db = api_db.set_index('index')
        api_db['country_code'] = api_db['country_code'].astype('category')
        api_db['continent'] = api_db['continent'].astype('category')
        api_db['store_type'] = api_db['store_type'].astype('category')
        api_db['locality'] = api_db['locality'].astype('string')
        api_db['store_code'] = api_db['store_code'].astype('string')
        api_db = api_db.drop('lat', axis=1)
        api_db = api_db[api_db['country_code'].str.len() < 4]
        
        def correct_continent(input):
            if len(input) <= 7:
                return input
            return input[2:]
        api_db['continent'] = api_db['continent'].apply(correct_continent)
        api_db['continent'] = api_db['continent'].astype('category')
        
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
            # print(day)
            # print(month)
            # print(year)
            return f"{year}-{month}-{day}"
        api_db['opening_date'] = api_db['opening_date'].apply(standardise_date)
        api_db['opening_date'] = pd.to_datetime(api_db['opening_date'], format="%Y-%m-%d")
        
        def standardise_storenumbers(numb):
            numb = ''.join(c for c in numb if c.isdigit())
            return numb
        
        api_db['staff_numbers'] = api_db['staff_numbers'].apply(standardise_storenumbers)
        api_db['staff_numbers'] = api_db['staff_numbers'].astype('int64')
        
        def standardise_address(addr):
            if len(addr) == 3:
                return addr
            addr = re.split('\n|,', addr)
            addr = addr[:-1]
            addr = ', '.join(addr)
            return addr
        
        api_db['address'] = api_db['address'].apply(standardise_address)
        api_db['address'] = api_db['address'].astype('string')
        
        api_db = api_db.replace('N/A', np.NaN)
        api_db.loc[0,['latitude']]=np.NaN
        
        def standardise_longlat(val):
            if val is None: return val
            return round(float(val), 2) if val != 'NaN' else 'NaN'
        
        api_db['longitude'] = api_db['longitude'].apply(standardise_longlat)
        api_db['latitude'] = api_db['latitude'].apply(standardise_longlat)
        
        return api_db
        
    def convert_product_weights(df):
        df = df[df['weight'].str.len() < 10]
        
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
        
        df['weight'] = df['weight'].apply(standardise_weight)
        return df
    
    def clean_products_data(df):
        df.rename(columns = {'Unnamed: 0':'index'}, inplace = True)
        df = df.set_index('index')
        df['product_price'] = df['product_price'].astype('string')
        
        def standardise_prod_price(price):
            price = price[1:]
            return float(price)
    
        df['product_price'] = df['product_price'].apply(standardise_prod_price)
        df['category'] = df['category'].astype('category')
        df['EAN'] = df['EAN'].astype('string')
        
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
            # print(day)
            # print(month)
            # print(year)
            return f"{year}-{month}-{day}"
    
        df['date_added'] = df['date_added'].apply(standardise_date)
        df['date_added'] = pd.to_datetime(df['date_added'], format="%Y-%m-%d")
        df['uuid'] = df['uuid'].astype('string')
        df['removed'] = df['removed'].astype('category')
        df['product_code'] = df['product_code'].astype('string')
        df['product_name'] = df['product_name'].astype('string')
        
        return df
      
    def clean_orders_data(orders):
        orders['date_uuid'] = orders['date_uuid'].astype('string')
        orders['user_uuid'] = orders['user_uuid'].astype('string')
        orders['store_code'] = orders['store_code'].astype('string')
        orders['product_code'] = orders['product_code'].astype('string')
        orders['card_number'] = orders['card_number'].astype('string')
        orders = orders.set_index('index')
        orders = orders.sort_index()
        orders = orders.drop(['level_0', 'first_name', 'last_name', '1'], axis = 1)
        return orders
    
    def clean_date_events_data(events):
        events = events[events['day'].str.len() <= 2]
        events['date_uuid'] = events['date_uuid'].astype('string')
        events['time_period'] = events['time_period'].astype('category')
        events['year'] = events['year'].astype('string')
        events['month'] = events['month'].astype('string')
        events['day'] = events['day'].astype('string')
                
        def pad_day_month(day):
            if len(day) == 2:
                return day
            else: return '0' + day
        events['day'] = events['day'].apply(pad_day_month)
        events['month'] = events['month'].apply(pad_day_month)
        
        events['date'] = events[['year', 'month', 'day']].agg('-'.join, axis=1)
        events['iso'] = events[['date', 'timestamp']].agg(' '.join, axis=1)
        events['iso'] = pd.to_datetime(events['iso'])
        events = events.drop(['date'], axis=1)
        return events
        
    
