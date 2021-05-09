# rd_data_engeneering
Training project for robot dreams

## Running

```bash
docker-compose run airflow-init
docker-compose up
```

Access link at [link](http://localhost:8080/) and run *shop_etl* pipeline.

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
