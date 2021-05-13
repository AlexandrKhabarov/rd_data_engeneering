from datetime import timedelta

from airflow import DAG
from airflow.hooks.base import BaseHook
from airflow.models import Variable
from airflow.operators.python import PythonOperator, PythonVirtualenvOperator
from airflow.utils.dates import days_ago

from out_of_stock_elt.main import main as out_of_stock_elt
from dshop_elt.main import main as dshop_elt

dump_out_of_stock_variables = Variable.get("dump_out_of_stock", deserialize_json=True)
dshop__postgres_connection = BaseHook.get_connection("dshop__postgres")
out_of_stock__api_connection = BaseHook.get_connection("out_of_stock__api")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['hub.sasha@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

execution_date = '{{ execution_date }}'

target_path = f"~/{dump_out_of_stock_variables['TARGET_PATH']}"

out_of_stock_path = f"{target_path}/out_of_stock"
aisles_path = f"{target_path}/aisles"
clients_path = f"{target_path}/clients"
departments_path = f"{target_path}/departments"
orders_path = f"{target_path}/orders"
products_path = f"{target_path}/products"

dag = DAG(
    dag_id="shop_etl",
    default_args=default_args,
    description='Shop ETL',
    schedule_interval=timedelta(seconds=10),
    start_date=days_ago(2),
    tags=['dshop'],
)

with dag:
    dump_out_of_stock = PythonVirtualenvOperator(
        task_id="dump_out_of_stock",
        python_callable=out_of_stock_elt,
        requirements=[
            "requests==2.25.1",
            "typing-extensions==3.7.4.3"
        ],
        op_args=[[
            "--AUTH_URL", dump_out_of_stock_variables['AUTH_URL'],
            "--USERNAME", out_of_stock__api_connection.login,
            "--PASSWORD", out_of_stock__api_connection.password,
            "--PRODUCT_URL", dump_out_of_stock_variables['PRODUCT_URL'],
            "--TARGET_PATH", out_of_stock_path,
            "--TIMEOUT", dump_out_of_stock_variables['TIMEOUT'],
            "--INGESTION_TIMESTAMP", execution_date,
            "--DATES", *dump_out_of_stock_variables['DATES'],
        ]],
    )
    dump_aisles = PythonOperator(
        task_id="dump_aisles",
        python_callable=dshop_elt,
        op_args=[
            "aisles",
            dshop__postgres_connection.schema,
            dshop__postgres_connection.host,
            dshop__postgres_connection.login,
            dshop__postgres_connection.password,
            aisles_path,
            execution_date,
        ]
    )
    dump_clients = PythonOperator(
        task_id="dump_clients",
        python_callable=dshop_elt,
        op_args=[
            "clients",
            dshop__postgres_connection.schema,
            dshop__postgres_connection.host,
            dshop__postgres_connection.login,
            dshop__postgres_connection.password,
            clients_path,
            execution_date,
        ]
    )

    dump_departments = PythonOperator(
        task_id="dump_departments",
        python_callable=dshop_elt,
        op_args=[
            "departments",
            dshop__postgres_connection.schema,
            dshop__postgres_connection.host,
            dshop__postgres_connection.login,
            dshop__postgres_connection.password,
            departments_path,
            execution_date,
        ]
    )

    dump_orders = PythonOperator(
        task_id="dump_orders",
        python_callable=dshop_elt,
        op_args=[
            "orders",
            dshop__postgres_connection.schema,
            dshop__postgres_connection.host,
            dshop__postgres_connection.login,
            dshop__postgres_connection.password,
            orders_path,
            execution_date,
        ]
    )

    dump_products = PythonOperator(
        task_id="dump_products",
        python_callable=dshop_elt,
        op_args=[
            "products",
            dshop__postgres_connection.schema,
            dshop__postgres_connection.host,
            dshop__postgres_connection.login,
            dshop__postgres_connection.password,
            products_path,
            execution_date,
        ]
    )
