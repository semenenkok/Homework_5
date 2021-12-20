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


def print_hello():
    return 'Hello world from first Airflow DAG!'

bitCoinRates = PythonOperator(
    task_id='hello_task', 
    python_callable=print_hello,
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