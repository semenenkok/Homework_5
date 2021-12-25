# from airflow.hooks.postgres_hook import PostgresHook
# pg_hook = PostgresHook(postgres_conn_id='postgres_bigishdata') 

# def write_to_pg(**kwargs):
#     execution_time = kwargs['ts']
#     run_time = dt.datetime.utcnow()
#     print('Writing to pg', runtime, execution_time)
#     dts_insert = 'insert into dts (runtime, execution_time) values (%s, %s)'
#     pg_hook.run(dts_insert, parameters=(runtime, execution_time,))


# dag = DAG('writing_to_pg',  # note the difference of the DAG name. This is important to keep separate for now, and also in the future with versions.
#           default_args=default_args,
#           start_date=dt.datetime.now(),
#           schedule_interval=dt.timedelta(seconds=10)
#           )