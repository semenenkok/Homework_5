from datetime import datetime, timedelta
# from textwrap import dedent
import requests
import json
import psycopg2


from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from airflow.providers.postgres.operators.postgres import PostgresOperator


def getbitcoinrate(**kwargs):
    ti = kwargs['ti']
    url = 'https://api.coincap.io/v2/rates/bitcoin'
    r = requests.get(url)
    r.encoding = 'utf-8'
    if r.status_code == 200:
        data = json.loads(r.text)

        # id = data['data']['id']
        # symbol = data['data']['symbol']
        # currencysymbol = data['data']['currencySymbol']
        # rateusd = data['data']['rateUsd']
        # type =  data['data']['type']

        #сохраняем в xcom
        ti.xcom_push(key='id',              value=data['data']['id']),
        ti.xcom_push(key='symbol',          value=data['data']['symbol']),
        ti.xcom_push(key='currencysymbol',  value=data['data']['currencysymbol']),
        ti.xcom_push(key='rateusd',         value=data['data']['rateusd']),
        ti.xcom_push(key='type',            value=data['data']['type']),
        
    else:
        print('api response code is: ' + str(r.status_code))
        raise


with DAG(
    dag_id='bitCoinRate_DAG',
    schedule_interval='*/30 * * * *',
    start_date=datetime(2021, 12, 20),
    catchup=False,
    tags=['HW5'],
    render_template_as_native_obj=True,  #https://airflow.apache.org/docs/apache-airflow/stable/concepts/operators.html#rendering-fields-as-native-python-objects
) as dag:

    create_bitcoinrate_table = PostgresOperator(
        task_id="create_bitcoinrate_table",
        postgres_conn_id="analytics",
        sql="""create table if not exists bitcoinrates2(
                    ts             timestamp with time zone default CURRENT_TIMESTAMP,
                    id             varchar(50),
                    symbol         varchar(50),
                    currencysymbol varchar(5),
                    rateusd        numeric(38, 16),
                    type           varchar(50));
            """,
    )


    getbitcoinrate = PythonOperator(
        task_id='getbitcoinrate', 
        python_callable=getbitcoinrate,
        dag=dag
    )

    
    insert_bitcoinrate = PostgresOperator(
        task_id="insert_bitcoinrate",
        postgres_conn_id="analytics",
        sql="""insert into bitcoinrates2 (id, symbol, currencysymbol, rateusd, type) 
               values  ('{{ ti.xcom_pull(task_ids='getbitcoinrate', key='id') }}',
                        '{{ ti.xcom_pull(task_ids='getbitcoinrate', key='symbol') }}', 
                        '{{ ti.xcom_pull(task_ids='getbitcoinrate', key='currencysymbol') }}',
                         {{ ti.xcom_pull(task_ids='getbitcoinrate', key='rateusd') }},
                        '{{ ti.xcom_pull(task_ids='getbitcoinrate', key='type') }}'
                        )""",
    )



create_bitcoinrate_table >> getbitcoinrate >> insert_bitcoinrate
