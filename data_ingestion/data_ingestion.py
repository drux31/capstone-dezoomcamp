#!/usr/bin/env python
# coding: utf-8
# author : drux31<contact@lnts.me>
# date : 2024-02-26
'''
python for data ingestion/extraction for the project
'''
import duckdb
from queries import (
    query_union_lieux, 
    query_union_usagers, 
    query_union_carac,
    query_union_vhl,
    query_dw_log_table,
    query_seq
)

def get_con(db_name):
    """Open a connection with duckdb"""
    con = duckdb.connect(db_name)
    return con

def create_staging(db_name):
    """read data from duckdb database"""
    print("------------------------------------Creating staging area----------------------------------")
    con = get_con(db_name)
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
    print("------------------------------------writing logs-------------------------------------------")
    con.sql(query_seq)
    con.sql(query_dw_log_table)

    print("Usagers: ")
    con.sql("insert into staging.dw_log_table\
            (schema_name, table_name, table_row_count, last_update) \
            select \
            'staging', \
            'usagers_all',\
             count(*), \
            '2021-12-31' \
            from staging.usagers_all")
    # con.sql("select count(*) from staging.usagers_all").show()

    #print("Lieux: ")
    con.sql("insert into staging.dw_log_table\
            (schema_name, table_name, table_row_count, last_update) \
            select \
            'staging', \
            'lieux_all',\
             count(*), \
            '2021-12-31' \
            from staging.lieux_all")
    #con.sql("select count(*) from staging.lieux_all").show()

    #print("Caractéristiques: ")
    con.sql("insert into staging.dw_log_table \
            (schema_name, table_name, table_row_count, last_update) \
            select \
            'staging', \
            'caracteristiques_all',\
            count(*), \
            '2021-12-31' \
            from staging.caracteristiques_all")
    #con.sql("select count(*) from staging.caracteristiques_all").show()

    #print("Véhicules: ")
    con.sql("insert into staging.dw_log_table \
            (schema_name, table_name, table_row_count, last_update) \
            select \
            'staging', \
            'vehicules_all',\
            count(*), \
            '2021-12-31' \
            from staging.vehicules_all")
    #con.sql("select count(*) from staging.vehicules_all").show()
    print("Log table: ")
    con.sql("select * from staging.dw_log_table").show()
    con.close()
    return 0

def get_daily_data(db_name):
    """reading daily data from duckdb datawarehouse"""
    con = duckdb.connect(db_name)
    res = con.sql("select distinct last_update \
            from staging.dw_log_table \
            order by 1 desc").fetchone()
    last_date = res[0]
    con.sql(f"select \
            count(Accident_Id) as nb_accident, \
            cast(concat(an, '-', cast(mois as BIGINT), '-', jour) as date) as Accident_date \
            from raw_data.caracteristiques_2022 \
            where cast(concat(an, '-', cast(mois as BIGINT), '-', jour) as date) = date '{last_date}' + 2 \
            group by 2"
             ).show()

    print(res[0])

def main():
    """
    main data ingestion function
    """
    db_name='../data/project_dw.db'
    get_daily_data(db_name)
    
if __name__ == "__main__":
    main()