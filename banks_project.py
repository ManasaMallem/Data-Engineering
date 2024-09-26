# Code for ETL operations on Country-GDP data

# Importing the required libraries
import requests
from bs4 import BeautifulSoup
import pandas as pd
import sqlite3
import numpy as np
from datetime import datetime


def log_progress(message):
    timestamp_format = '%Y-%h-%d-%H:%M:%S' # Year-Monthname-Day-Hour-Minute-Second 
    now = datetime.now() # get current timestamp 
    timestamp = now.strftime(timestamp_format) 
    with open("./code_log.txt","a") as f: 
        f.write(timestamp + ' : ' + message + '\n') 


def extract(url, table_attribs):
    #Extracting webpage as text
    page=requests.get(url).text
    #parsing text into an html object
    data=BeautifulSoup(page,'html.parser')
    #creating an empty dataframe 'df' with columns as table_attribs
    df=pd.DataFrame(columns=table_attribs)
    #Extracting all 'tbody' attribs of html object
    tables=data.find_all('tbody')
    #Extracting all rows of indx 2 table using 'tr' attrib
    rows = tables[0].find_all('tr')
     #Checking contents of each row having attrib 'td' for conditions
      #a. The row should not be empty
      #b. Removing last character from last column
    for row in rows:
        col = row.find_all('td')
        if len(col) != 0:
            data_dict = {"Name": col[1].find_all("a")[1]["title"],
                         "MC_USD_Billion": float(col[2].contents[0][:-1])}
            df1 = pd.DataFrame(data_dict, index=[0])
            df = pd.concat([df, df1], ignore_index=True)
    return df

def transform(df, csv_path):
    # Step 1: Read the exchange rate CSV file
    exchange_rate = pd.read_csv(csv_path)
    
    # Step 2: Convert the CSV data into a dictionary with currency codes as keys and exchange rates as values
    exchange_rate = exchange_rate.set_index('Currency').to_dict()['Rate']
    
    # Step 3: Add 3 new columns with conversion to GBP, EUR, and INR
    df['MC_GBP_Billion'] = [np.round(x * exchange_rate['GBP'], 2) for x in df['MC_USD_Billion']]
    df['MC_EUR_Billion'] = [np.round(x * exchange_rate['EUR'], 2) for x in df['MC_USD_Billion']]
    df['MC_INR_Billion'] = [np.round(x * exchange_rate['INR'], 2) for x in df['MC_USD_Billion']]
    # Return the updated DataFrame
    return df

def load_to_csv(df, output_path):
    df.to_csv(output_path)

def load_to_db(df, sql_connection, table_name):
   df.to_sql(table_name, sql_connection, if_exists='replace',index=False)

def run_query(query_statement, sql_connection):
    print(query_statement)
    query_output = pd.read_sql(query_statement, sql_connection)
    print(query_output)
    

#Function Calls
url='https://web.archive.org/web/20230908091635 /https://en.wikipedia.org/wiki/List_of_largest_banks'
csv_path='./exchange_rate.csv'
table_attribs=['Name','MC_USD_Billion']
#final_table_attribs=['Name','MC_USD_Billion', 'MC_GBP_Billion', 'MC_EUR_Billion', 'MC_INR_Billion']
output_path='./Largest_banks.csv'
db_name='Banks.db'
table_name='Largest_banks'
log_file='./code_log.txt'

log_progress('Preliminaries complete. Initiating ETL process')

df=extract(url,table_attribs)
log_progress('Data extraction complete. Initiating Transformation process')

df=transform(df,csv_path)
log_progress('Data transformation complete. Initiating Loading process')

load_to_csv(df, output_path)
log_progress('Data saved to CSV file')

sql_connection=sqlite3.connect('Banks.db')
log_progress('SQL Connection initiated')

load_to_db(df, sql_connection, table_name)
log_progress('Data loaded to Database as a table, Executing queries')

query_statement = f"SELECT * from {table_name}"
run_query(query_statement, sql_connection)

query_statement=f"SELECT AVG(MC_GBP_Billion) FROM {table_name}"
run_query(query_statement, sql_connection)

query_statement=f"SELECT Name FROM {table_name} LIMIT 5"
run_query(query_statement, sql_connection)

query_statement=f"SELECT Name FROM {table_name} LIMIT 6"
run_query(query_statement, sql_connection)

log_progress('Process Complete.')

sql_connection.close()
log_progress("Server Connection closed")

# Task 7: Verify log entries
with open(log_file, "r") as log:
    LogContent = log.read()
    print(LogContent)