import sqlite3
import requests
import numpy
import pandas as pd
import os
from bs4 import BeautifulSoup
from datetime import timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago

# defining DAG arguments

default_args = {
    'owner': 'Roberto A.',
    'start_date': days_ago(0),
    'email': ['dummy@email.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    dag_id='etl_process_dag',
    default_args=default_args,
    description='Code Challenge Airflow',
    schedule_interval=timedelta(days=1)
)

# defining tasks definitions

def extract_bank_data():
    """
    Goes into given url and web scrapes data from largest banks worldwide.
    Extract just required data from a table and dumps into a CSV file
    which is stored on the parent directory of this python file (opt/airflow)
    """
    url = 'https://web.archive.org/web/20230908091635%20/https://en.wikipedia.org/wiki/List_of_largest_banks'
    request = requests.get(url)
    if request.status_code == 200:
        soup = BeautifulSoup(request.text, features="html.parser")
        all_tables = soup.find_all('tbody')
        DATA = {
            'Name': [],
            'MC_USD_Billion': []
        }

        for table in all_tables:
            headers = table.find_all('th')
            if 'Market cap' in headers[len(headers)-1]:
                market_table = table
                break

        for row in market_table.find_all('tr')[1:]:
            name_column = row.find_all('td')[1]
            print(row.find_all('td')[2].contents[0][:-1])
            DATA['Name'].append(name_column.find_all('a')[1].text.replace('\n',''))
            DATA['MC_USD_Billion'].append(row.find_all('td')[2].text.replace('\n',''))
        
        dataframe = pd.DataFrame(DATA)
        try:
            dataframe.to_csv("List_of_largest_banks.csv", index = False)
        except FileExistsError:
            os.remove("List_of_largest_banks.csv")
            dataframe.to_csv("List_of_largest_banks.csv", index = False)
    else:
        print("The server is not reachable")

def extract_exchange_rate():
    """
    Requests a csv file from a given url and extracs the data to generate a local CSV
    which is which is stored on the parent directory of this python file (opt/airflow)
    """
    url = 'https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBMSkillsNetwork-PY0221EN-Coursera/labs/v2/exchange_rate.csv'
    csv = requests.get(url, allow_redirects=True)
    with open('ex_rate_file.csv', 'wb') as file:
        file.write(csv.content)

def transform():
    """
    Retrieves the data stored on the CSV files generated on previous extraction steps
    transforms it and generates extra data. Then dumps it on a CSV file which is stored on the parent directory of this python file (opt/airflow)
    """
    bank_data_df = pd.read_csv("List_of_largest_banks.csv")
    ex_rates_df = pd.read_csv("ex_rate_file.csv")
    bank_data_df['MC_USD_Billion'] = numpy.float64(bank_data_df['MC_USD_Billion'])
    for rates in range(0, len(ex_rates_df)):
        column_name = f'MC_{ex_rates_df["Currency"].iloc[rates]}_Billion'
        bank_data_df[column_name] = round(bank_data_df['MC_USD_Billion']*ex_rates_df['Rate'].iloc[rates],2)
    try:
        bank_data_df.to_csv("transformed_data.csv", index = False)
    except FileExistsError:
        os.remove("transformed_data.csv")
        bank_data_df.to_csv("transformed_data.csv", index = False)
        
def load_to_db():
    """
    Generates a sqlite like db file, then gets data from the transformed_data CSV file and dumps it into DB file
    called bank_data.db which is stored on the parent directory of this python file (opt/airflow)
    """
    db_name = "bank_data.db"
    try:
        with open(db_name, "x"):
            pass
    except FileExistsError:
        os.rmdir(db_name)
        with open(db_name, "x"):
            pass
    transformed_data_df = pd.read_csv("transformed_data.csv")
    db_conn = sqlite3.connect(db_name)
    transformed_data_df.to_sql(con = db_conn, name = "transformed_data", if_exists = 'replace', index = False)
    db_conn.close()

# Defining task operators

bank_data_web_scraping = PythonOperator(task_id = "bank_data_web_scraping", python_callable = extract_bank_data, dag = dag)
exchange_rate_file = PythonOperator(task_id = "get_exchange_rate", python_callable = extract_exchange_rate, dag = dag)
transforming_data = PythonOperator(task_id = "converting_currencies", python_callable = transform, dag = dag)
loading_to_sqlite = PythonOperator(task_id = "loading_to_DB", python_callable = load_to_db, dag = dag)

# tasks dependencies/pipeline

bank_data_web_scraping.set_downstream(transforming_data)
exchange_rate_file.set_downstream(transforming_data)
transforming_data.set_downstream(loading_to_sqlite)
