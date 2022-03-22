# from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
#
# from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.postgres_operator import PostgresOperator

# from dock_assgn_create import create_sql_table
# from dock_assgn_insert import insert_into_sql_table


default_args = {
    "owner": "Harshita",
    "depends_on_past": False,
    "start_date": datetime(2022, 3, 20),
    "email": ["airflow@airflow.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5)
}

dag = DAG("Dag", default_args=default_args, schedule_interval="0 6 * * *")

# t1 = PythonOperator(task_id='create_sql_table', python_callable=create_sql_table, dag=dag)
# t2 = PythonOperator(task_id='insert_sql_table', python_callable=insert_into_sql_table, dag=dag)
t1 = DummyOperator(task_id="start_task", dag= dag)
t2 = PostgresOperator(task_id="create_new_table_in_postgre", postgres_conn_id='postgres_conn', \
                     sql="sql/create_new_table.sql", dag=dag)
t3 = DummyOperator(task_id="end_task", dag=dag)

t1 >> t2 >> t3
