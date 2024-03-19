#!/usr/bin/env python
# coding: utf-8
# author : drux31<contact@lnts.me>
# date : 2024-03-18
'''
python for data ingestion/extraction for the project
- DATASET : https://www.data.gouv.fr/en/datasets/\
            bases-de-donnees-annuelles-des-\
            accidents-corporels-de-la-circulation-\
            routiere-annees-de-2005-a-2022/
'''
import requests
import os
import duckdb


def get_file(filename, url, year):
    """
    Download the file and save it locally for future use
    """
    response = requests.get(url, stream=True)
    response.raise_for_status() #raise an HTTPError for bad responses
    
    with open(f'../data/{year}/{filename}', 'wb') as fd:
        #n = 1
        for chunk in response.iter_content(chunk_size=1024):
            fd.write(chunk)
    if os.path.isfile(f'../data/{year}/{filename}'):
        return True
    raise FileNotFoundError(f"The file {filename} does not exist")
    

def web_to_local(file_name, db_name):
    """
    Download the file and save it locally for future use
    """
    with open(file_name, encoding='utf-8') as f:
        for line in f:
            year, service, url = line.strip('\n ').split(',')
            print(year, service, url)
           
            if not os.path.isdir(f'../data/{year}'):
                os.mkdir(f'../data/{year}') # create the folder
            else:
                filename = f'{service}_{year}.csv'
                if get_file(filename, url, year):
                    #create a connection to the db
                    con = duckdb.connect(db_name)
                    schema_name = 'raw_data'
                    table_name = filename.split('.')[0]
                    con.sql(f"CREATE SCHEMA IF NOT EXISTS {schema_name};")
                    con.sql(f"CREATE TABLE IF NOT EXISTS {schema_name}.{table_name}" 
                            + " AS SELECT * "
                            + f" FROM read_csv('../data/{year}/{filename}');")
                    con.table(f"{schema_name}.{table_name}").show()
                    con.close()
                    #print(filename)        
        return 0
    raise Exception("Problem during the creation of the database")
    

def main(file_name):
    """
    main data ingestion function
    """
    #years = ['2022', '2021', '2020', '2019']
    db_name = 'project.db'
    web_to_local(file_name, db_name)
    
if __name__ == "__main__":
    file_name = '../data/dataset.txt'
    main(file_name)