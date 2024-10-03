from airflow import DAG
from airflow.decorators import task
from airflow.utils.dates import days_ago
from airflow.models import Variable
import requests
import snowflake.connector
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
import pandas as pd
from datetime import datetime


#basic Info of the operator

#The Dag

####def return_snowflake_conn():
####
####    hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
####    
####    # Execute the query and fetch results
####    conn = hook.get_conn()
####    return conn.cursor()

with DAG(
    dag_id='fetch_stock_data',
    default_args={'owner': 'deekshachauhan7', 'start_date': days_ago(1)},
    schedule_interval='@daily',
    catchup=False
) as dag:


        @task
        def fetch_data():
            # Fetch the last 90 days of stock data from Alpha Vantage API
            api_url = Variable.get("url")
            response = requests.get(api_url)
            data = response.json()
            return data

        @task
        def process_fetched_data(data):
            results = []
            for d in data["Time Series (Daily)"]:
                stock_info = data["Time Series (Daily)"][d]
                stock_info["date"] = d
                results.append(stock_info)
            return results
                
        


        @task
        def create_or_replace_table():
            conn = snowflake.connector.connect(
                user=Variable.get("snowflake_username"),
                password=Variable.get("snowflake_password"),
                account=Variable.get("snowflake_account"),
                database=Variable.get("snowflake_database"),
                schema='raw_data'
            )
            cursor = conn.cursor()
            cursor.execute('BEGIN')
            try:
                cursor.execute("""
                    CREATE OR REPLACE TABLE raw_data.stock_data (
                        date STRING PRIMARY KEY,
                        open FLOAT,
                        high FLOAT,
                        low FLOAT,
                        close FLOAT,
                        volume FLOAT,
                        symbol STRING
                    )
                """)
                cursor.execute('COMMIT')
            
            except Exception as e:
                conn.rollback()
                print(f"An error occurred: {e}")



        @task
        def load_to_snowflake(df):
            # Load processed data into Snowflake with try/except and SQL transaction
            conn = snowflake.connector.connect(
                user=Variable.get("snowflake_username"),
                password=Variable.get("snowflake_password"),
                account=Variable.get("snowflake_account"),
                database=Variable.get("snowflake_database"),
                schema='raw_data'
            )
            cursor = conn.cursor()
            cursor.execute('BEGIN')
            try:        
                insert_query = '''
                INSERT INTO raw_data.stock_data (date, open, high, low, close, volume, symbol)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                '''
                cursor = conn.cursor()
                for row in df:
                    print(row)
                    cursor.execute(insert_query, (
                        row['date'], row['1. open'], row['2. high'], row['3. low'],
                        row['4. close'], row['5. volume'], 'TSLA'
                    ))

                conn.cursor().execute("COMMIT")
                print("Data inserted successfully.")
            except Exception as e:
                conn.cursor().execute("ROLLBACK")
                print(f"Error inserting data: {e}")

        # Define task dependencies
        data = fetch_data()
        fixed_data = process_fetched_data(data)
        create_or_replace_table() >> process_fetched_data(data) >> load_to_snowflake(fixed_data)
