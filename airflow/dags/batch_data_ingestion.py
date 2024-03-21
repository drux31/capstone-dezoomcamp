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

from queries import (
    query_union_lieux, 
    query_union_usagers, 
    query_union_carac,
    query_union_vhl,
    query_dw_log_table,
    query_seq
)

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
    
    @task(task_id="Initial_load")
    def create_staging():
        """read data from duckdb database"""
        db_name = '/home/drux/de_project/capstone-dezoomcap/data/project_dw.db'
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
    
    @task(task_id="Writing_logs")
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
    db_name = create_staging()
    write_log(db_name)
process_bacth_data()