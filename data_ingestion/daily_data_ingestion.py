#!/usr/bin/env python
# coding: utf-8
# author : drux31<contact@lnts.me>
# date : 2024-02-26
'''
python for data ingestion/extraction for the project
'''
import duckdb
from queries import (
    query_carac_2022, 
    query_vhl_2022,
    q_lieux_2022
)

def get_con(db_name):
    """Open a connection with duckdb"""
    con = duckdb.connect(db_name)
    return con

def get_daily_data(db_name):
    """reading daily data from duckdb datawarehouse"""
    print("------------------------------ ingesting daily data ------------------------")
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

    print("------------------------------ creation completed for staging area ------------------------")
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


def main():
    """
    main data ingestion function
    """
    db_name='../data/project_dw.db'
    get_daily_data(db_name)
    
if __name__ == "__main__":
    main()