import requests
import sqlite3
import pandas as pd 
import numpy as np 
from datetime import datetime
from bs4 import BeautifulSoup

# Log Message Function
def log_progress(message): 
    timestamp_format = '%Y-%h-%d-%H:%M:%S' # Year-Monthname-Day-Hour-Minute-Second 
    now = datetime.now() # get current timestamp 
    timestamp = now.strftime(timestamp_format) 
    with open("./code_log.txt","a") as f: 
        f.write(timestamp + ' : ' + message + '\n')

# Extraction Function
def extract(url, table_attribs):
    html_page = requests.get(url).text
    data = BeautifulSoup(html_page, 'html.parser')
    df = pd.DataFrame(columns = table_attribs)
    tables = data.find_all('tbody')
    rows = tables[0].find_all('tr')
    for row in rows:
        col = row.find_all('td')
        if len(col) != 0:
            name = col[1].text.strip() 
            mc_usd = col[2].text.strip() 
            if name is not None and mc_usd is not None:
                data_dict = {'Name': name, 'MC_USD_Billions': mc_usd}
                df1 = pd.DataFrame(data_dict, index=[0])
                df = pd.concat([df, df1], ignore_index=True)
    return df

# Transformation Function
def transform(df, table_attribs, path):
    exchange_rate_df = pd.read_csv(path)
    dict_rate = exchange_rate_df.set_index('Currency').to_dict()['Rate']
    # set usd column to numeric value
    df[table_attribs[1]] = pd.to_numeric(df[table_attribs[1]], errors='coerce')
    # use usd value to set other columns
    df[table_attribs[2]] = df[table_attribs[1]]*dict_rate['GBP']
    df[table_attribs[3]] = df[table_attribs[1]]*dict_rate['EUR']
    df[table_attribs[4]] = df[table_attribs[1]]*dict_rate['INR']
    df[table_attribs[2:]] = df[table_attribs[2:]].round(2)
    return df

# Load To CSV Function
def load_to_csv(df, csv_path):
    df.to_csv(csv_path)
    
# Load to db
def load_to_db(df, sql_conn, table_name):
    df.to_sql(table_name, sql_connection, if_exists='replace', index=False)

# Query Function
def run_query(query_statement, sql_connection):
    print(query_statement)
    query_output = pd.read_sql(query_statement, sql_connection)
    print(query_output)

# Main
if __name__ == '__main__':
    # Initializations
    url = 'https://web.archive.org/web/20230908091635%20/https://en.wikipedia.org/wiki/List_of_largest_banks'
    db_name = 'Banks.db'
    table_name = 'Largest_banks'
    csv_path = '/home/project/Largest_banks_data.csv'
    exchange_path = '/home/project/exchange_rate.csv'
    initial_table_attribs = ['Name', 'MC_USD_Billions']
    new_table_attribs = ['Name', 'MC_USD_Billions', 'MC_GBP_Billion', 'MC_EUR_Billion', 'MC_INR_Billion']
    log_progress('Preliminaries complete. Initiating ETL process')

    # Extract
    df = extract(url, initial_table_attribs)
    print("Extraction function output")
    print(df)
    log_progress('Data extraction complete. Initializing Transformation process.')

    # Transform
    df = transform(df, new_table_attribs, exchange_path)
    print('Transform Done')
    print(df)
    log_progress('Data transformation complete. Initializing Loading process')

    # Load csv 
    load_to_csv(df, csv_path)
    log_progress('Data saved to csv')

    # Connect to DB
    sql_connection = sqlite3.connect(db_name)
    log_progress('SQL connection initiated')

    # Load to DB
    load_to_db(df, sql_connection, table_name)
    log_progress('Data loaded to Database as table, Executing queries')

    # Run query A
    print("Query A")
    query_statementa = f"SELECT * FROM {table_name}"
    run_query(query_statementa, sql_connection)

    # Run query B
    print("Query B")
    query_statementb = f"SELECT AVG(MC_GBP_Billion) FROM {table_name}"
    run_query(query_statementb, sql_connection)

    # Run query C
    print("Query C")
    query_statementc = f"SELECT NAME from {table_name} LIMIT 5"
    run_query(query_statementc, sql_connection)
    log_progress('Process Complete.')

    # exit program
    sql_connection.close()
    log_progress('Server Connection closed')
