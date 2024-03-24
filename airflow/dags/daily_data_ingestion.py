#!/usr/bin/env python
# coding: utf-8
# author : drux31<contact@lnts.me>
# date : 2024-02-26
'''
python for daily data ingestion from raw_schema
'''
import os
import duckdb
import datetime
import pendulum

from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator
from google.cloud import storage, bigquery

from queries import (
    query_carac_2022, 
    query_vhl_2022,
    q_lieux_2022
)

DATA_FOLDER = os.environ["DATA_FOLDER"]
PATH_TO_DBT_PROJECT = os.environ["DBT_PROJECT_FOLDER"]

@dag(
        dag_id="process-daily-data-2022",
        #schedule_interval="0 0 * * *",
        schedule="@daily",
        start_date=pendulum.datetime(2024, 3, 21, tz="UTC"),
        catchup=False,
        dagrun_timeout=datetime.timedelta(minutes=5),
)
def process_daily_data():
    """DAG for processing daily data from raw schema to staging"""
    @task(task_id="Daily_data", 
          multiple_outputs=True
          )
    def get_daily_data():
        """
        reading daily data from duckdb datawarehouse
        from raw_data schema to staging
        """
        print("------------------------------ ingesting daily data ------------------------")
        db_name = f'{DATA_FOLDER}/project_dw.db'
        con = duckdb.connect(db_name)
        res = con.sql("select distinct last_update \
                from staging.dw_log_table \
                order by 1 desc").fetchone()
        last_date = res[0]
        query_id_of_the_day = f"""
                select
                Accident_Id as Num_Acc
                from raw_data.caracteristiques_2022
                where cast(concat(an, '-', \
                        cast(mois as BIGINT), '-', \
                                jour) as date) = date '{last_date}' + 1
                """
        #con.sql(query_id_of_the_day).show()

        daily_usager_data = f"""
                insert into staging.usagers_all
                select 
                        Num_Acc,
                        id_vehicule,
                        num_veh,
                        place,
                        catu,
                        grav,
                        sexe,
                        an_nais,
                        trajet,
                        secu1,
                        secu2,
                        secu3,
                        locp,
                        actp,
                        etatp,
                        id_usager
                from raw_data.usagers_2022
                where Num_Acc in (
                        {query_id_of_the_day}
                )
                """
        daily_carac_data = f"""
                insert into staging.caracteristiques_all
                {query_carac_2022}
                where Num_Acc in (
                        {query_id_of_the_day}
                )
                """
        daily_lieux_data = f"""
                insert into staging.lieux_all
                {q_lieux_2022}
                where Num_Acc in (
                        {query_id_of_the_day}
                )
                """
        daily_vhl_data = f"""
                insert into staging.vehicules_all
                {query_vhl_2022}
                where Num_Acc in (
                        {query_id_of_the_day}
                )
                """
        con.sql(daily_usager_data)    
        con.sql(daily_carac_data)
        con.sql(daily_lieux_data)
        con.sql(daily_vhl_data)
        con.close()
        print("------------------------------ ingestion completed for staging area ------------------------")
        return {"db_name": db_name, "last_date": last_date}

    @task(task_id="Staging_to_GCS")
    def load_staging_to_gcs(db_name: str):
        print("------------------------writing to GCS----------------------------")
        print("----------------------writing local parquet-----------------------")
        # The name for the bucket
        bucket_name = "dez-capstone-bucket"
        db_name = f'{DATA_FOLDER}/project_dw.db'

        # Instantiate a client
        storage_client = storage.Client()
        bucket = storage_client.get_bucket(bucket_name)

        # The ID of your new GCS object
        usagers_blob_name = bucket.blob("daily_load/usagers_data.parquet")
        lieux_blob_name = bucket.blob("daily_load/lieux_data.parquet")
        vehicules_blob_name = bucket.blob("daily_load/vehicules_data.parquet")
        caracteristiques_blob_name = bucket.blob("daily_load/caracteristiques_data.parquet")       

        usagers_filename = f"{DATA_FOLDER}/usagers_data.parquet"
        
        caracteristiques_filename = f"{DATA_FOLDER}/caracteristiques_data.parquet"
        
        lieux_filename = f"{DATA_FOLDER}/lieux_data.parquet"
        
        vehicules_filename = f"{DATA_FOLDER}/vehicules_data.parquet"

        con = duckdb.connect(db_name)
        #write the parquet file
        print("---------------------Usagers----------------------")
        # usager_all
        con.sql(
            "select * from staging.usagers_all"
            ).write_parquet(usagers_filename)
        
        print("----------------------Lieux-----------------------")
        # lieux_all
        con.sql(
            "select * from staging.lieux_all"
            ).write_parquet(lieux_filename)
        
        print("----------------------Caracteristiques---------------")
        # lieux_all
        con.sql(
            "select * from staging.caracteristiques_all"
            ).write_parquet(caracteristiques_filename)
        
        print("----------------------Vehicules-----------------------")
        # lieux_all
        con.sql(
            "select * from staging.vehicules_all"
            ).write_parquet(vehicules_filename)
        con.close()

        print("----------------------upload parquet to gcs-----------------------")        
        usagers_blob_name.upload_from_filename(usagers_filename)
        lieux_blob_name.upload_from_filename(lieux_filename)
        vehicules_blob_name.upload_from_filename(vehicules_filename)
        caracteristiques_blob_name.upload_from_filename(caracteristiques_filename)
        
        print(f"file {usagers_filename} uploaded to {usagers_blob_name}")
        print(f"file {vehicules_filename} uploaded to {vehicules_blob_name}")
        print(f"file {caracteristiques_filename} uploaded to {caracteristiques_blob_name}")
        print(f"file {lieux_filename} uploaded to {lieux_blob_name}")
        return 0

    @task(task_id="gcs_to_bigquery")
    def gcs_to_bq():
        # connect to duckdb
        #con = duckdb.connect('project_dw.db')
        #write the parquet file
        #con.sql("select * from staging.usagers_all").write_csv("out.csv")

        # Construct a BigQuery client object.
        client = bigquery.Client()

        # TODO(developer): Set table_id to the ID of the table to create.
        #table_id = "drux-de-zoomcamp.staging.usagers_all"

        job_config = bigquery.LoadJobConfig(
                source_format=bigquery.SourceFormat.PARQUET, 
                autodetect=True,
                write_disposition="WRITE_TRUNCATE",
        )

        #with open("out.csv", "rb") as source_file:
        #    job = client.load_table_from_file(source_file, table_id, job_config=job_config)
        uris = {
            "drux-de-zoomcamp.staging.usagers_all": "gs://dez-capstone-bucket/daily_load/usagers_data.parquet",
            "drux-de-zoomcamp.staging.lieux_all": "gs://dez-capstone-bucket/daily_load/lieux_data.parquet",
            "drux-de-zoomcamp.staging.vehicules_all": "gs://dez-capstone-bucket/daily_load/vehicules_data.parquet",
            "drux-de-zoomcamp.staging.caracteristiques_all": "gs://dez-capstone-bucket/daily_load/caracteristiques_data.parquet"
        }
        #"gs://cloud-samples-data/bigquery/us-states/us-states.parquet"
        for k, uri in uris.items():
            load_job = client.load_table_from_uri(
                uri, k, job_config=job_config
            )  # Make an API request.
            load_job.result()  # Waits for the job to complete.

            #table = client.get_table(table_id)  # Make an API request.
            table = client.get_table(k)

            #print("Loaded {} rows.".format(destination_table.num_rows))
            print(
                "Loaded {} rows and {} columns to {}".format(
                    table.num_rows, 
                    len(table.schema), 
                    k
                )
            )
        return 0

    @task(task_id="Writing_logs")
    def write_log(db_name: str, last_date: str):
        """
        Write the logs into the table dw_log_table
        """
        print("------------------------------------writing logs-------------------------------------------")
        con = duckdb.connect(db_name)
        print("------------------------------------writing logs-------------------------------------------")
        con.sql(f"insert into staging.dw_log_table\
                (schema_name, table_name, table_row_count, last_update) \
                select \
                'staging', \
                'usagers_all',\
                count(*), \
                date '{last_date}' + 1 \
                from staging.usagers_all")
        con.sql(f"insert into staging.dw_log_table\
                (schema_name, table_name, table_row_count, last_update) \
                select \
                'staging', \
                'lieux_all',\
                count(*), \
                date '{last_date}' + 1 \
                from staging.lieux_all")
        con.sql(f"insert into staging.dw_log_table \
                (schema_name, table_name, table_row_count, last_update) \
                select \
                'staging', \
                'caracteristiques_all',\
                count(*), \
                date '{last_date}' + 1  \
                from staging.caracteristiques_all")
        con.sql(f"insert into staging.dw_log_table \
                (schema_name, table_name, table_row_count, last_update) \
                select \
                'staging', \
                'vehicules_all',\
                count(*), \
                date '{last_date}' + 1 \
                from staging.vehicules_all")
        print("Log table: ")
        con.sql(f"select * \
                from staging.dw_log_table \
                where last_update in ('{last_date}', date '{last_date}' + 1) ").show()
        con.close()
        print("------------------------------ Writing logs completed ------------------------")
        return 0
    
    # t1, t2 and t3 are examples of tasks created by instantiating operators
    t1 = BashOperator(
        task_id="test_dbt",
        cwd=PATH_TO_DBT_PROJECT,
        bash_command="dbt test",
    )

    t2 = BashOperator(
        task_id="build_dbt",
        bash_command="dbt build",
        cwd=PATH_TO_DBT_PROJECT,
        retries=2,
    )

    res = get_daily_data()
    [write_log(res["db_name"], res["last_date"]), load_staging_to_gcs(res["db_name"]) >> gcs_to_bq() >> t1 >> t2]
process_daily_data()
    
