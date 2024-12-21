import pytz
import yaml
from datetime import datetime
from airflow.models.param import Param
from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from resources.scripts.Dags_Assigment.extract_dwh import extract_table
from resources.scripts.Dags_Assigment.load_dwh import load_table

with open("dags/resources/config/Dags_Assigment.yaml", "r") as f:
   config = yaml.safe_load(f)

#### Menjalankan DAGS setiap 2 menit di hari ini full
@dag(
   schedule_interval = "*/5 * * * *",  # Menjalankan setiap 2 menit
   start_date        = datetime(2024, 12, 21, tzinfo=pytz.timezone("Asia/Jakarta")),  # Mulai dari hari ini
   end_date          = datetime(2024, 12, 21, 23, 59, 59, tzinfo=pytz.timezone("Asia/Jakarta")),  # Selesai di akhir hari ini
   catchup           = False,  # Tidak melakukan catch-up
   params            = {
       table: Param("incremental", description="incremental / full", enum=["full", "incremental"])
       for table in config["ingestion"]
   }
)


def Dags_Assigment():
   start_task          = EmptyOperator(task_id="start_task")
   end_task            = EmptyOperator(task_id="end_task")
   wait_el_task        = EmptyOperator(task_id="wait_el_task")
   wait_transform_task = EmptyOperator(task_id="wait_transform_task")

   # Membuat task EL (Extract & Load) secara dinamis berdasarkan konfigurasi
   for table in config.get("ingestion", []):
       extract = task(extract_table, task_id=f"extract.{table}")
       load    = task(load_table, task_id=f"load.{table}")

       start_task >> extract(table) >> load(table) >> wait_el_task

   # Membuat task transformasi secara dinamis berdasarkan konfigurasi
   for filepath in config.get("transformation", []):
       transform = SQLExecuteQueryOperator(
           task_id = f"transform.{filepath.split('/')[-1]}",
           conn_id = "postgres_dibimbing",
           sql     = filepath,
       )

       wait_el_task >> transform >> wait_transform_task

   # Membuat task datamart secara dinamis berdasarkan konfigurasi
   for filepath in config.get("datamart", []):
       datamart = SQLExecuteQueryOperator(
           task_id = f"datamart.{filepath.split('/')[-1]}",
           conn_id = "postgres_dibimbing",
           sql     = filepath,
       )

       wait_transform_task >> datamart >> end_task

Dags_Assigment()







