from datetime import datetime, timedelta
from textwrap import dedent
import requests
import json
import psycopg2


from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['k.semenenko@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
    # 'wait_for_downstream': False,
    # 'dag': dag,
    # 'sla': timedelta(hours=2),
    # 'execution_timeout': timedelta(seconds=300),
    # 'on_failure_callback': some_function,
    # 'on_success_callback': some_other_function,
    # 'on_retry_callback': another_function,
    # 'sla_miss_callback': yet_another_function,
    # 'trigger_rule': 'all_success'
}

dag = DAG(
    dag_id='bitCoinRates',
    default_args=default_args,
    description='BitCoinRates loader DAG',
    schedule_interval='*/30 * * * *',
    start_date=datetime(2021, 12, 20),
    catchup=False,
    tags=['homework 5'],
)


def main():
    url = 'https://api.coincap.io/v2/rates/bitcoin'
    r = requests.get(url)
    r.encoding = 'utf-8'
    data = json.loads(r.text)

    id = data['data']['id']
    symbol = data['data']['symbol']
    currencysymbol = data['data']['currencySymbol']
    rateUsd = data['data']['rateUsd']
    type =  data['data']['type']

    insert_bitcoinRates(id, symbol, currencysymbol, rateUsd, type)



def insert_bitcoinRates(id, symbol, currencysymbol, rateusd, type):
    try:
        conn = psycopg2.connect(host="rc1c-6aq36ytblcrw3avn.mdb.yandexcloud.net",
                    database="analytics",
                    user="semen", 
                    password="OTUSBESTCOURCES", 
                    port="6432",
                    target_session_attrs="read-write",
                    sslmode="verify-full"
                    )
        # conn = psycopg2.connect("host=localhost dbname=analytics user=postgres password=3321")
        cur = conn.cursor()
        #cur.execute("CREATE TABLE test (id serial PRIMARY KEY, num integer, data varchar);")
        cur.execute("insert into bitcoinrates (id, symbol, currencysymbol, rateusd, type) VALUES (%s, %s, %s, %s, %s)", (id, symbol, currencysymbol, rateusd, type))

        conn.commit()
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)   
    finally:
        if conn is not None:
            conn.close()


bitCoinRates = PythonOperator(
    task_id='bitCoinRates', 
    python_callable=main,
    dag=dag)

bitCoinRates.doc_md = dedent(
        """\
    #### Task Documentation
    Bla bla bla.
    ![img](http://montcs.bloomu.edu/~bobmon/Semesters/2012-01/491/import%20soul.png)
    """
    )

dag.doc_md = __doc__  # providing that you have a docstring at the beginning of the DAG
dag.doc_md = """
    This is a documentation placed anywhere
    """  # otherwise, type it like this
templated_command = dedent(
        """
    {% for i in range(5) %}
        echo "{{ ds }}"
        echo "{{ macros.ds_add(ds, 7)}}"
        echo "{{ params.my_param }}"
    {% endfor %}
    """
)

bitCoinRates