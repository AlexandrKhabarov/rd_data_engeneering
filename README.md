# rd_data_engeneering
Training project for robot dreams

## Running

```bash
docker-compose run airflow-init
docker-compose up
```

Create connection in Airflow Web UI to postgres with dshop db.

Setup variables for *out_of_stock_etl* job in Airflow Web UI.

Access [link](http://localhost:8080/) and run *shop_etl* pipeline.

## How to start development locally

Install development requirements:

```bash
pip install -r common/development.txt
```

Install production requirements:

```bash
pip install -r common/production.txt
```

## How to check

Run pytest in the root of the project

```bash
pytest
```
