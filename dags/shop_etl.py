from datetime import timedelta

from airflow import DAG
from airflow.operators.python import PythonVirtualenvOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.dates import days_ago

from out_of_stock_elt.main import main

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['hub.sasha@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    dag_id="shop_etl",
    default_args=default_args,
    description='Shop ETL',
    schedule_interval=timedelta(days=1),
    start_date=days_ago(2),
    tags=['dshop'],
)

dump_out_of_stock = PythonVirtualenvOperator(
    task_id="dump_out_of_stock",
    python_callable=main,
    requirements=[
        "requests==2.25.1",
        "PyYAML==5.4.1",
        "pydantic==1.8.1",
    ],
    op_args=[["--config", "./config.yaml"]],
    dag=dag,
)
dump_aisles = PostgresOperator(
    task_id="dump_aisles",
    sql=r"COPY aisles TO 'C:\tmp\aisles.csv' DELIMITER ',' CSV HEADER;",
    postgres_conn_id="dshop__postgres",
    dag=dag
)
dump_clients = PostgresOperator(
    task_id="dump_clients",
    sql=r"COPY clients TO 'C:\tmp\clients.csv' DELIMITER ',' CSV HEADER;",
    postgres_conn_id="dshop__postgres",
    dag=dag
)

dump_departments = PostgresOperator(
    task_id="dump_departments",
    sql=r"COPY departments TO 'C:\tmp\departments.csv' DELIMITER ',' CSV HEADER;",
    postgres_conn_id="dshop__postgres",
    dag=dag
)

dump_orders = PostgresOperator(
    task_id="dump_orders",
    sql=r"COPY orders TO 'C:\tmp\orders.csv' DELIMITER ',' CSV HEADER;",
    postgres_conn_id="dshop__postgres",
    dag=dag
)

dump_products = PostgresOperator(
    task_id="dump_products",
    sql=r"COPY products TO 'C:\tmp\products.csv' DELIMITER ',' CSV HEADER;",
    postgres_conn_id="dshop__postgres",
    dag=dag
)
