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
    query_union_vhl
)

def get_con(db_name):
    """Open a connection with duckdb"""
    con = duckdb.connect(db_name)
    return con

def create_staging(db_name):
    """read data from duckdb database"""
    con = get_con(db_name)
    ## Create the staging schema
    con.sql("CREATE SCHEMA IF NOT EXISTS staging")
    
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
    
    print("Row counts for all the tables")
    print("Usagers: ")
    con.sql("select count(*) from staging.usagers_all").show()
    print("Lieux: ")
    con.sql("select count(*) from staging.lieux_all").show()
    print("Caractéristiques: ")
    con.sql("select count(*) from staging.caracteristiques_all").show()
    print("Véhicules: ")
    con.sql("select count(*) from staging.vehicules_all").show()
    con.close()
    return 0

def main():
    """
    main data ingestion function
    """
    db_name='project_dw.db'
    create_staging(db_name)
    
if __name__ == "__main__":
    main()