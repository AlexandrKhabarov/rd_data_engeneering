def main(table_name, dbname, host, user, password, path, ingestion_timestamp):
    import os

    import psycopg2

    with_timestamp = os.path.join(
        path,
        "ingestion_timestamp=%s" % ingestion_timestamp
    )

    os.makedirs(with_timestamp, exist_ok=True)

    with_table_name = os.path.join(
        with_timestamp,
        f"{table_name}.csv"
    )

    sql = f"COPY (SELECT * FROM {table_name}) TO STDOUT WITH DELIMITER ',' CSV HEADER;"

    conn = psycopg2.connect(
        host=host,
        database=dbname,
        user=user,
        password=password
    )

    with conn.cursor() as cur:
        with open(with_table_name, "w") as file:
            cur.copy_expert(sql, file)
