# def extract_table(table, **kwargs):
#    import os
#    import pandas as pd
#    from airflow.providers.mysql.hooks.mysql import MySqlHook

#    data_interval_start = kwargs['data_interval_start']
#    data_interval_end   = kwargs['data_interval_end']
#    ingestion_mode      = kwargs["params"][table]

#    engine = MySqlHook("mysql_dibimbing").get_sqlalchemy_engine()
#    with engine.connect() as conn:

#        # Mengambil kolom dengan tipe data terkait timestamp dari tabel yang ditentukan
#        timestamp_cols = pd.read_sql(f"""
#            SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS
#            WHERE TABLE_SCHEMA = 'rsvs_movie'
#                AND DATA_TYPE IN ('date', 'datetime', 'timestamp')
#                AND TABLE_NAME = '{table}'
#        """, conn).COLUMN_NAME.tolist()

#        print("Kolom timestamp:", timestamp_cols)

#        # Membuat query SQL untuk mengekstrak data dari tabel dengan menambahkan kondisi tambahan
#        query = f"SELECT *, CURRENT_TIMESTAMP AS md_extracted_at FROM rsvs_movie.{table}"
#        if ingestion_mode == "incremental" and timestamp_cols:
#            query += " WHERE "
#            query += " OR ".join(f"{col} BETWEEN '{data_interval_start}' AND '{data_interval_end}'" for col in timestamp_cols)

#        print("Query:", query)

#        # Menjalankan query SQL dan memuat hasilnya ke dalam DataFrame pandas
#        df = pd.read_sql(query, conn)
#        print("DataFrame:", df)

#    os.makedirs(f"data/data_rsvs_movie/{table}", exist_ok=True)
#    df.to_parquet(f"data/data_rsvs_movie/{table}/{data_interval_start}_{data_interval_end}_{ingestion_mode}.parquet", index=False)


def extract_table(table, **kwargs):
    """
    Fungsi untuk mengekstrak data dari tabel MySQL berdasarkan mode ingestion.
    
    Args:
        table (str): Nama tabel yang akan diekstrak.
        kwargs (dict): Argument tambahan, termasuk:
            - data_interval_start (str): Waktu mulai interval data.
            - data_interval_end (str): Waktu akhir interval data.
            - params (dict): Parameter tambahan seperti mode ingestion.
    """
    import os
    import pandas as pd
    import logging
    from pathlib import Path
    from airflow.providers.mysql.hooks.mysql import MySqlHook

    # Setup logging
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)

    # Argument tambahan
    data_interval_start = kwargs['data_interval_start']
    data_interval_end = kwargs['data_interval_end']
    ingestion_mode = kwargs["params"][table]

    # Inisialisasi koneksi database
    try:
        engine = MySqlHook("mysql_dibimbing").get_sqlalchemy_engine()
        with engine.connect() as conn:
            # Mengambil kolom timestamp dari tabel
            timestamp_query = f"""
                SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_SCHEMA = 'rsvs_movie'
                    AND DATA_TYPE IN ('date', 'datetime', 'timestamp')
                    AND TABLE_NAME = '{table}'
            """
            timestamp_cols = pd.read_sql(timestamp_query, conn).COLUMN_NAME.tolist()
            logger.info("Kolom timestamp: %s", timestamp_cols)

            # Membuat query SQL berdasarkan mode ingestion
            query = f"SELECT *, CURRENT_TIMESTAMP AS md_extracted_at FROM rsvs_movie.{table}"
            if ingestion_mode == "incremental" and timestamp_cols:
                conditions = " OR ".join(
                    f"{col} BETWEEN '{data_interval_start}' AND '{data_interval_end}'" for col in timestamp_cols
                )
                query += f" WHERE {conditions}"

            logger.info("Query: %s", query)

            # Eksekusi query dan muat data ke DataFrame
            df = pd.read_sql(query, conn)
            logger.info("Jumlah baris data yang diekstrak: %d", len(df))

    except Exception as e:
        logger.error("Terjadi kesalahan saat mengekstrak data: %s", e)
        return

    # Menyimpan data ke file parquet
    try:
        output_dir = Path(f"data/data_rsvs_movie/{table}")
        output_dir.mkdir(parents=True, exist_ok=True)
        output_file = output_dir / f"{data_interval_start}_{data_interval_end}_{ingestion_mode}.parquet"
        df.to_parquet(output_file, index=False)
        logger.info("Data berhasil disimpan ke: %s", output_file)

    except Exception as e:
        logger.error("Terjadi kesalahan saat menyimpan data: %s", e)
