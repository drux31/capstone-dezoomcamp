#!/usr/bin/env python
# coding: utf-8
# author : drux31<contact@lnts.me>
# date : 2024-02-26
'''
python for daily data ingestion from raw_schema
'''
import duckdb
import datetime
import pendulum
from airflow.decorators import dag, task

from queries import (
    query_carac_2022, 
    query_vhl_2022,
    q_lieux_2022
)

@dag(
        dag_id="process-daily-data-2022",
        schedule_interval="0 0 * * *",
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
        db_name = '/home/drux/de_project/capstone-dezoomcap/data/project_dw.db'
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
    res = get_daily_data()
    write_log(res["db_name"], res["last_date"])
process_daily_data()
    
