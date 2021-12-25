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

        id = data['data']['id']
        symbol = data['data']['symbol']
        currencysymbol = data['data']['currencySymbol']
        rateusd = data['data']['rateUsd']
        type =  data['data']['type']

        ti.xcom_push(value=id, key='id')
        ti.xcom_push(value=symbol, key='symbol')
        ti.xcom_push(value=currencysymbol, key='currencysymbol')
        ti.xcom_push(value=rateusd, key='rateusd')
        ti.xcom_push(value=type, key='type')

        # return [id, symbol, currencysymbol, type]
    else:
        print('api response code is: ' + str(r.status_code))
        raise

def print_data(**kwargs):
    ti = kwargs['ti']
    print('Полученные значения: {0}'.format(ti.xcom_pull(task_ids='getbitcoinrate')))



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

    # print_data = PythonOperator(
    #     task_id='print_data', 
    #     python_callable=print_data,
    #     dag=dag
    # )

    
    insert_bitcoinrate = PostgresOperator(
        task_id="insert_bitcoinrate",
        postgres_conn_id="analytics",
        sql="""insert into bitcoinrates2 (id, symbol, currencysymbol, rateusd, type) 
               values  ("{{ ti.xcom_pull(task_ids='getbitcoinrate', key='id') }}",
                        "{{ ti.xcom_pull(task_ids='getbitcoinrate', key='symbol') }}", 
                        "{{ ti.xcom_pull(task_ids='getbitcoinrate', key='currencysymbol') }}"
                        "{{ ti.xcom_pull(task_ids='getbitcoinrate', key='rateusd') }}"
                        "{{ ti.xcom_pull(task_ids='getbitcoinrate', key='type') }}"
                        )""",
    )



    # task_insert_new_row = PostgresOperator(
    #             task_id='insert_new_row',
    #             trigger_rule=TriggerRule.ALL_DONE,
    #             sql='''INSERT INTO table_name VALUES (%s, '{{ ti.xcom_pull(task_ids='execute_bash', key='return_value') }}', %s);''',
    #             parameters=(uuid.uuid4().int % 123456789, datetime.now()))


create_bitcoinrate_table >> getbitcoinrate >> insert_bitcoinrate


# def insert_bitcoinRates(id, symbol, currencysymbol, rateusd, type):
#     conn = None
#     try:
#         env = {'filename':'database.ini',
#                'section':'yandexCloud'
#         }

#         params = config(**env)
#         conn = psycopg2.connect(**params)
#     except (Exception, psycopg2.DatabaseError) as err:
#         print("Database connection error: {0}".format(err))
#         raise

#     if conn is not None:
#         try:
#             cur = conn.cursor()
#             cur.execute("insert into bitcoinrates (id, symbol, currencysymbol, rateusd, type) VALUES (%s, %s, %s, %s, %s)", (id, symbol, currencysymbol, rateusd, type))
#             conn.commit()
#             cur.close()
#         except (Exception) as error:
#             print(error)   
#             raise
#         finally:
#             conn.close()









# bitCoinRates.doc_md = dedent(
#         """\
#     #### Task Documentation
#     Bla bla bla.
#     ![img](http://montcs.bloomu.edu/~bobmon/Semesters/2012-01/491/import%20soul.png)
#     """
#     )

# dag.doc_md = __doc__  # providing that you have a docstring at the beginning of the DAG
# dag.doc_md = """
#     This is a documentation placed anywhere
#     """  # otherwise, type it like this
# templated_command = dedent(
#         """
#     {% for i in range(5) %}
#         echo "{{ ds }}"
#         echo "{{ macros.ds_add(ds, 7)}}"
#         echo "{{ params.my_param }}"
#     {% endfor %}
#     """
# )

