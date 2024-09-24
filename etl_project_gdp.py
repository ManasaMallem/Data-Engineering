# Code for ETL operations on Country-GDP data

# Importing the required libraries
from bs4 import BeautifulSoup
import requests
import pandas as pd
import numpy as np
import sqlite3
from datetime import datetime 

def extract(url, table_attribs):
     #Extracting webpage as text
    page = requests.get(url).text
    #Parsing text into an html objecct
    data = BeautifulSoup(page,'html.parser')
    #Creating an empty dataframe 'df' with columns as table_attribs
    df = pd.DataFrame(columns=table_attribs)
    #Extracting all 'tbody' attribs of html object
    tables = data.find_all('tbody')
    #Extracting all rows of index 2 table using 'tr' attrib
    rows = tables[2].find_all('tr')
     #Checking contents of each row having attrib 'td' for conditions
      #a. The row should not be empty
      #b. The first col should contain a hyperlink
      #c. The third col should not be '__'
    for row in rows:
        col = row.find_all('td')
        if len(col)!=0:
            if col[0].find('a') is not None and '—' not in col[2]:
                data_dict = {"Country": col[0].a.contents[0],
                             "GDP_USD_millions": col[2].contents[0]}
                df1 = pd.DataFrame(data_dict, index=[0])
                df = pd.concat([df,df1], ignore_index=True)
    return df

def transform(df):
    #Converting 'GDP_USD_millions' pf df from currency to floating numbers
    GDP_list = df["GDP_USD_millions"].tolist()
    GDP_list = [float("".join(x.split(','))) for x in GDP_list]
    #Dividing all values by 1000 and round it to 2 decimal places
    GDP_list = [np.round(x/1000,2) for x in GDP_list]
    df["GDP_USD_millions"] = GDP_list
    #Modifying column name from 'GDP_USD_millions' to 'GDP_USD_billions'
    df=df.rename(columns = {"GDP_USD_millions":"GDP_USD_billions"})
    return df

def load_to_csv(df, csv_path):
    df.to_csv(csv_path)

def load_to_db(df, sql_connection, table_name):
    df.to_sql(table_name, sql_connection, if_exists='replace', index=False)

def run_query(query_statement, sql_connection):
    print(query_statement)
    query_output = pd.read_sql(query_statement, sql_connection)
    print(query_output)

def log_progress(message):
    timestamp_format = '%Y-%h-%d-%H:%M:%S' # Year-Monthname-Day-Hour-Minute-Second 
    now = datetime.now() # get current timestamp 
    timestamp = now.strftime(timestamp_format) 
    with open("./etl_project_log.txt","a") as f: 
        f.write(timestamp + ' : ' + message + '\n')    

#Function calls
url = 'https://web.archive.org/web/20230902185326/https://en.wikipedia.org/wiki/List_of_countries_by_GDP_%28nominal%29'
table_attribs = ["Country", "GDP_USD_millions"]
db_name = 'World_Economies.db'
table_name = 'Countries_by_GDP'
csv_path = './Countries_by_GDP.csv'

log_progress('Preliminaries complete. Initiating ETL process')

df = extract(url, table_attribs)
log_progress('Data extraction complete. Initiating Transformation process')

df = transform(df)
log_progress('Data transformation complete. Initiating loading process')

load_to_csv(df, csv_path)
log_progress('Data saved to CSV file')

sql_connection = sqlite3.connect('World_Economies.db')
log_progress('SQL Connection initiated.')

load_to_db(df, sql_connection, table_name)
log_progress('Data loaded to Database as table. Running the query')

query_statement = f"SELECT * from {table_name} WHERE GDP_USD_billions >= 100"
run_query(query_statement, sql_connection)
log_progress('Process Complete.')

sql_connection.close()