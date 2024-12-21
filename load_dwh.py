# def load_table(table, **kwargs):
#    import pandas as pd
#    from airflow.providers.postgres.hooks.postgres import PostgresHook

#    data_interval_start = kwargs['data_interval_start']
#    data_interval_end   = kwargs['data_interval_end']
#    ingestion_mode      = kwargs["params"][table]

#    df = pd.read_parquet(f"data/case_study/{table}/{data_interval_start}_{data_interval_end}_{ingestion_mode}.parquet")

#    engine = PostgresHook("postgres_dibimbing").get_sqlalchemy_engine()
#    with engine.connect() as conn:
#        # Menentukan mode penulisan ke tabel berdasarkan tipe sinkronisasi (incremental atau replace)
#        if ingestion_mode == "incremental":
#            if_exists = "append"
#        else:
#            if_exists = "replace"

#        # Menulis DataFrame ke tabel SQL di schema "bronze" dengan mode yang ditentukan
#        df.to_sql(table, conn, schema="bronze", index=False, if_exists=if_exists)



import logging

def load_table(table, **kwargs):
    import pandas as pd
    from airflow.providers.postgres.hooks.postgres import PostgresHook

    logging.info(f"Task started for table: {table}")
    data_interval_start = kwargs.get('data_interval_start')
    data_interval_end   = kwargs.get('data_interval_end')
    ingestion_mode      = kwargs.get("params", {}).get(table)

    logging.info(f"data_interval_start: {data_interval_start}, data_interval_end: {data_interval_end}, ingestion_mode: {ingestion_mode}")

    file_path = f"data/data_rsvs_movie/{table}/{data_interval_start}_{data_interval_end}_{ingestion_mode}.parquet"
    logging.info(f"Looking for file: {file_path}")

    try:
        df = pd.read_parquet(file_path)
        logging.info(f"File loaded successfully: {file_path}")
    except FileNotFoundError as e:
        logging.error(f"File not found: {file_path}")
        raise e

    engine = PostgresHook("postgres_dibimbing").get_sqlalchemy_engine()
    with engine.connect() as conn:
        if ingestion_mode == "incremental":
            if_exists = "append"
        else:
            if_exists = "replace"

        logging.info(f"Writing DataFrame to SQL with mode: {if_exists}")
        df.to_sql(table, conn, schema="report_log", index=False, if_exists=if_exists)
        logging.info(f"Data written to table: {table} in schema: report_log")
