#!/usr/bin/env python
# coding: utf-8
# author : drux31<contact@lnts.me>
# date : 2024-02-26
'''
python code for batch data ingestion from raw_data schema
'''
import duckdb
import datetime
import pendulum
from airflow.decorators import dag, task
from google.cloud import storage, bigquery
import os

from queries import (
    query_union_lieux, 
    query_union_usagers, 
    query_union_carac,
    query_union_vhl,
    query_dw_log_table,
    query_seq
)

DATA_FOLDER = os.environ["DATA_FOLDER"]

@dag(
        dag_id="process-batch-data-2019-2021",
        schedule=None,
        start_date=pendulum.datetime(2019, 1, 1, tz="UTC"),
        catchup=False,
        dagrun_timeout=datetime.timedelta(minutes=5),
)
def process_bacth_data():
    '''
    This DAG will load all the accidents data between 2019 and 2022
    in a single batch
    '''
    
    @task(task_id="initial_loads")
    def create_staging():
        """read data from duckdb database"""
        db_name = f'{DATA_FOLDER}/project_dw.db'
        print("------------------------------------Creating staging area----------------------------------")
        con = duckdb.connect(db_name)
        ## Create the staging schema
        con.sql("DROP SCHEMA IF EXISTS staging CASCADE")
        con.sql("CREATE OR REPLACE SCHEMA staging")
        
        # Load the usagers data
        con.sql("create or replace table staging.usagers_all as ("
                + query_union_usagers + ")")
                
        #Load the lieux data  
        con.sql("create or replace table staging.lieux_all as ("
                + query_union_lieux + ")")
                
        #Load caracteristiques data
        con.sql("create or replace table staging.caracteristiques_all as ("
                + query_union_carac + ")")
        
        # Load vehicules data
        con.sql("create or replace table staging.vehicules_all as ("
                + query_union_vhl + ")")
        print("------------------------------ creation completed for staging area ------------------------")
        con.close()
        return db_name
    

    @task(task_id="staging_to_gcs")
    def load_staging_to_gcs(db_name: str):
        print("------------------------writing to GCS----------------------------")
        print("----------------------writing local parquet-----------------------")
        # The name for the bucket
        bucket_name = "dez-capstone-bucket"

        # Instantiate a client
        storage_client = storage.Client()
        bucket = storage_client.get_bucket(bucket_name)

        # The ID of your new GCS object
        usagers_blob_name = bucket.blob("initial_load/usagers_data.parquet")
        lieux_blob_name = bucket.blob("initial_load/lieux_data.parquet")
        vehicules_blob_name = bucket.blob("initial_load/vehicules_data.parquet")
        caracteristiques_blob_name = bucket.blob("initial_load/caracteristiques_data.parquet")       

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
            "drux-de-zoomcamp.staging.usagers_all": "gs://dez-capstone-bucket/initial_load/usagers_data.parquet",
            "drux-de-zoomcamp.staging.lieux_all": "gs://dez-capstone-bucket/initial_load/lieux_data.parquet",
            "drux-de-zoomcamp.staging.vehicules_all": "gs://dez-capstone-bucket/initial_load/vehicules_data.parquet",
            "drux-de-zoomcamp.staging.caracteristiques_all": "gs://dez-capstone-bucket/initial_load/caracteristiques_data.parquet"
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


    @task(task_id="writing_logs")
    def write_log(db_name: str):
        print("------------------------------------writing logs-------------------------------------------")
        con = duckdb.connect(db_name)
        con.sql(query_seq)
        con.sql(query_dw_log_table)

        con.sql("insert into staging.dw_log_table\
                (schema_name, table_name, table_row_count, last_update) \
                select \
                'staging', \
                'usagers_all',\
                count(*), \
                '2021-12-31' \
                from staging.usagers_all")
        con.sql("insert into staging.dw_log_table\
                (schema_name, table_name, table_row_count, last_update) \
                select \
                'staging', \
                'lieux_all',\
                count(*), \
                '2021-12-31' \
                from staging.lieux_all")
        con.sql("insert into staging.dw_log_table \
                (schema_name, table_name, table_row_count, last_update) \
                select \
                'staging', \
                'caracteristiques_all',\
                count(*), \
                '2021-12-31' \
                from staging.caracteristiques_all")
        con.sql("insert into staging.dw_log_table \
                (schema_name, table_name, table_row_count, last_update) \
                select \
                'staging', \
                'vehicules_all',\
                count(*), \
                '2021-12-31' \
                from staging.vehicules_all")
        print("Log table: ")
        con.sql("select * from staging.dw_log_table").show()
        con.close()
        return 0
    
    db_name = create_staging() # f'{DATA_FOLDER}/project_dw.db'
    [load_staging_to_gcs(db_name) >> gcs_to_bq(), write_log(db_name)]
process_bacth_data()