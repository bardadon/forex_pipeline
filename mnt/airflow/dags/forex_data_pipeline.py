from email import message
from multiprocessing import context
from socket import timeout
from xml.dom import HierarchyRequestErr
from airflow import DAG
from airflow.providers.http.sensors.http import HttpSensor
from airflow.sensors.filesystem import FileSensor
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.hive_operator import HiveOperator
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from airflow.operators.email import EmailOperator
from airflow.contrib.operators.slack_webhook_operator import SlackWebhookOperator
from airflow.hooks.base_hook import BaseHook

from datetime import datetime, timedelta
import json
import requests
import csv

default_args = {
    "owner":"airflow",
    "email_on_failure": False,
    "email_on_retry": False,
    "email":"admin@localhost.com",
    "retries":1,
    "retry_delay":timedelta(minutes=5) 
}


# Download forex rates according to the currencies we want to watch
# described in the file forex_currencies.csv
def _download_rates():
    BASE_URL = "https://gist.githubusercontent.com/marclamberti/f45f872dea4dfd3eaa015a4a1af4b39b/raw/"
    ENDPOINTS = {
        'USD': 'api_forex_exchange_usd.json',
        'EUR': 'api_forex_exchange_eur.json'
    }
    with open('/opt/airflow/dags/files/forex_currencies.csv') as forex_currencies:
        reader = csv.DictReader(forex_currencies, delimiter=';')
        for idx, row in enumerate(reader):
            base = row['base']
            with_pairs = row['with_pairs'].split(' ')
            indata = requests.get(f"{BASE_URL}{ENDPOINTS[base]}").json()
            outdata = {'base': base, 'rates': {}, 'last_update': indata['date']}
            for pair in with_pairs:
                outdata['rates'][pair] = indata['rates'][pair]
            with open('/opt/airflow/dags/files/forex_rates.json', 'a') as outfile:
                json.dump(outdata, outfile)
                outfile.write('\n')


def _get_message():
    return "Hi from forex_data_pipeline"

SLACK_CONN_ID = 'slack_conn'




with DAG('forex_data_pipeline', start_date=datetime(2021, 1, 1), 
schedule_interval="@daily", default_args=default_args, catchup=False ) as dag:

    # Dag #1 - Check availability of forex rates
    is_forex_rates_available = HttpSensor(
        task_id = 'is_forex_rates_available', # Name of the DAG
        http_conn_id= 'forex_api',  # Name of the connection
        endpoint='marclamberti/f45f872dea4dfd3eaa015a4a1af4b39b', # Where to check connections
        response_check=lambda response: 'rates' in response.text, # Check if "rates" is in the response
        poke_interval = 5, 
        timeout = 20
    )

    # Dag #2 - Is the file forex_currencies.csv available
    is_forex_currencies_file_available = FileSensor(
        task_id = 'is_forex_currencies_file_available',
        fs_conn_id = 'forex_path', # Name of the connection
        filepath =  'forex_currencies.csv', # relative path to the file(in the connection, we specified that the path is:opt/airflow/dags/files, so the relavtive path to the file is just the file name)
        poke_interval = 5,
        timeout = 20
    )

    # Dag #3 - Download Rates
    download_rates = PythonOperator(
        task_id = 'download_rates',
        python_callable = _download_rates
    )


    # Dag #4 - Store rates in HDFS
    # First Command - Create a directory called forex
    # Second Command - Copy the JSON file from $AIRFLOW_HOME/dags/files/forex_rates.json to /forex 
    saving_rates = BashOperator(
        task_id = 'saving_rates',
        bash_command ="""
            hdfs dfs -mkdir -p /forex && \
            hdfs dfs -put -f $AIRFLOW_HOME/dags/files/forex_rates.json /forex
        """
    )   

    # Dag #5 - Create a Hive table
    creating_forex_rates_table = HiveOperator(
        task_id="creating_forex_rates_table",
        hive_cli_conn_id="hive_conn",
        hql="""

            drop table if exists forex_rates;

            CREATE EXTERNAL TABLE forex_rates(
                base STRING,
                last_update DATE,
                eur DOUBLE,
                usd DOUBLE,
                nzd DOUBLE,
                gbp DOUBLE,
                jpy DOUBLE,
                cad DOUBLE
                )
            ROW FORMAT DELIMITED
            FIELDS TERMINATED BY ','
            STORED AS TEXTFILE
        """
    )

    # Dag #6 - Process rates
    forex_processing = SparkSubmitOperator(
        task_id="forex_processing",
        application="/opt/airflow/dags/scripts/forex_processing.py",
        conn_id="spark_conn",
        verbose=False
    )


    # Dag #7 - Send email
    send_email_notification = EmailOperator(
        task_id = 'send_email_notification',
        to='bdadon50@gmail.com',
        subject='Forex Pipeline',
        html_content="<h3>Forex Pipeline</h3>"
    )

    # Dag #8 - Send slack
    send_slack_notification = SlackWebhookOperator(
        task_id="send_slack_notification",
        http_conn_id="slack_conn",
        message=_get_message(),
        channel="#monitoring"
    )

    # Dependencies
    is_forex_rates_available >> is_forex_currencies_file_available >> download_rates >> saving_rates
    saving_rates >> creating_forex_rates_table >> forex_processing >> send_email_notification >> send_slack_notification