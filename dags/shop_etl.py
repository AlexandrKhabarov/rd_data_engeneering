from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

from src.main import main

dag = DAG(
    dag_id="shop_etl"
)

dump_out_of_stock = PythonOperator(
    python_callable=main,
    op_args=[["--config", "./config.yaml"]],
    dag=dag,

)
dump_aisles = PostgresOperator(
    sql=r"COPY aisles TO 'C:\tmp\aisles.csv' DELIMITER ',' CSV HEADER;",
    postgres_conn_id="dshop__postgres",
    dag=dag

)
dump_clients = PostgresOperator(
    sql=r"COPY clients TO 'C:\tmp\clients.csv' DELIMITER ',' CSV HEADER;",
    postgres_conn_id="dshop__postgres",
    dag=dag

)

dump_departments = PostgresOperator(
    sql=r"COPY departments TO 'C:\tmp\departments.csv' DELIMITER ',' CSV HEADER;",
    postgres_conn_id="dshop__postgres",
    dag=dag

)

dump_orders = PostgresOperator(
    sql=r"COPY orders TO 'C:\tmp\orders.csv' DELIMITER ',' CSV HEADER;",
    postgres_conn_id="dshop__postgres",
    dag=dag

)

dump_products = PostgresOperator(
    sql=r"COPY products TO 'C:\tmp\products.csv' DELIMITER ',' CSV HEADER;",
    postgres_conn_id="dshop__postgres",
    dag=dag

)
