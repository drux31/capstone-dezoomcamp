# DE Zoomcamp - Capstone project: A study on car accidents in France between 2019 and 2022
![Tools landscape](images/tools_list.png)

This repository contains the implementation of my capstone project, from participating in the Data engineering zoomcamp, 2024 cohort. The project is about analyzing the progression of car accidents in France, from 2019 to 2022, with a focus on the evolution of the number of deceased people. 

The data will be fetched from a web open data platform, processed with and loadded into **DuckDB** as a local datawarehouse (for staging and dev area for data transformation), transformed using **dbt-core** and pushed into **BigQuery** as a production data warehouse, and then visualised using **Apache Superset**. 

We will use **Apache Airflow** as an orchestrator for the dbt-core production build, and also to orchestrate the batch data processing. **Terraform** will be used to set up the cloud environment (we're will be working on GCP), and **Docker** will enable us to contenairise our code so that it could run anywhere (easing the reproductibility). 

## About the project
The project is divided in four main parts: 
1. data extraction ;
2. data ingestion ;
3. data transformation ;
4. data visualisation.

### Data extraction
The dataset we are working with is extracted from the French government open data platform, with a focus on the data produced between 2019 and 2022. Since we are working with structured data only, there is no need to set up a data lake for object storage. Then the data will be extracted using a python script and stored localy in DuckDB, used as a local datawarehouse.

**Tools used for this process** : 

&rarr; simple python script for downloading the data ;

&rarr; DuckDB as local datawarehouse where we are storing the dowloaded data (we will create a schema called raw_data that will contain the conrresponding data).

The extraction script will be executed only once.

The scripts can be found in the data_ingestion folder and be ran either using the Makefile or directly inthe terminal.
1. directly in the terminal:
    * first we need to run the test to ensure everything is good :
    ```
    python -m pytest tests
    ```

    * then run the extraction script
    ```
    cd data_ingestion;./data_extraction.py
    ```

2. using the Makefile
The ```Makefile``` is built in a way that the tests will always be ran before the excution. And if something fails, the execution will not start. You just need one command:
```
make
```

In both cases, you will end up with a DuckDB database named ```project_dw.db```, that will all the data stored in it.

Architecture :
![alt Architexture for data extraction](images/data_extraction.png)

### Data ingestion
In this part, we will use Airflow to use the data in two pipelines to the staging area:
* a first pipeline that will load a  batch of data between 2019 and 2021 ;
* and another pipeline that will load the data from 2022 daily ; we are doing this since the dataset is updated once a year, so we are manually simulating a daily update from the 2022 data into the data warehouse.

We xill need to install airflow to be able to complete the ingestion process.

1.  Installing Airflow

Following the [documentation](https://airflow.apache.org/docs/apache-airflow/stable/start.html), we will install airflow using pip. Our configuration is the following:
* setting airflow home : ```export AIRFLOW_HOME=~/de_project/capstone-dezoomcap/airflow```

* install airflow:
```
AIRFLOW_VERSION=2.8.3

# Extract the version of Python you have installed. If you're currently using a Python version that is not supported by Airflow, you may want to set this manually.
# See above for supported versions.
PYTHON_VERSION="$(python --version | cut -d " " -f 2 | cut -d "." -f 1-2)"

CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"
# For example this would install 2.8.3 with python 3.8: https://raw.githubusercontent.com/apache/airflow/constraints-2.8.3/constraints-3.8.txt

pip install "apache-airflow==${AIRFLOW_VERSION}" --constraint "${CONSTRAINT_URL}"
```