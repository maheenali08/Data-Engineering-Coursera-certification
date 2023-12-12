
import pandas as pd 
import numpy as np 
from bs4 import BeautifulSoup 
import sqlite3 
import requests
from datetime import datetime 


# Code for ETL operations on Country-GDP data
url= 'https://web.archive.org/web/20230908091635%20/https://en.wikipedia.org/wiki/List_of_largest_banks'
table_attribs=['Name','MC_USD_Billion']
csv_path='Countries_by_GDP.csv'
db_name='Banks.db'
table_name='/.Largest_banks'
log_file='code_log.txt'
sql_connection = sqlite3.connect(db_name)
# Importing the required libraries
 

def extract(url,table_attribs):   
    page = requests.get(url).text
    data = BeautifulSoup(page,'html.parser')
    df = pd.DataFrame(columns=table_attribs)
    tables = data.find_all('tbody')
    rows = tables[0].find_all('tr')
    for row in rows:
        col = row.find_all('td')             
        print(col)
        if len(col)!=0: 
            data_dict = {"Name": col[1].contents[1],
                                "MC_USD_Billion": float(col[2].contents[0])}          
            df1 = pd.DataFrame(data_dict, index=[0])
            df = pd.concat([df,df1], ignore_index=True)
            df = df.replace('\n','', regex=True)
            
    return df
    
def transform():
    df['MC_GBP_Billion'] = [np.round(x*exchange_rate['GBP'],2) for x in df['MC_USD_Billion']]
    df['MC_EUR_Billion'] = [np.round(x*exchange_rate['GBP'],2) for x in df['MC_USD_Billion']]
    df['MC_INR_Billion'] = [np.round(x*exchange_rate['GBP'],2) for x in df['MC_USD_Billion']]

    GDP_list = df["GDP_USD_millions"].tolist()
    GDP_list = [float("".join(x.split(','))) for x in GDP_list]
    GDP_list = [np.round(x/1000,2) for x in GDP_list]
    df["GDP_USD_millions"] = GDP_list
    df=df.rename(columns = {"GDP_USD_millions":"GDP_USD_billions"})
    return df


def log_progress(message):
    timestamp_format = '%Y-%h-%d-%H:%M:%S' # Year-Monthname-Day-Hour-Minute-Second 
    now = datetime.now() # get current timestamp 
    timestamp = now.strftime(timestamp_format) 
    with open("./etl_project_log.txt","a") as f: 
        f.write(timestamp + ' : ' + message + '\n')   

def load_to_csv(df,csv_path):
    df.to_csv(csv_path)

def load_to_db(sql_connection,df,table_name):
    df.to_sql(table_name, sql_connection, if_exists='replace', index=False)

def run_queries(query_statement,sql_connection):
    print(query_statement)
    query_output = pd.read_sql(query_statement, sql_connection)
    print(query_output)



log_progress('Preliminaries complete. Initiating ETL process and calling Extraction')

extract(url,table_attribs)
