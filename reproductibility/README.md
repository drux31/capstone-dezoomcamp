## Reproductibility

### Data extraction

The scripts can be found in the data_ingestion folder and can be ran either using the Makefile or directly inthe terminal.

1. directly in the terminal:
    * first we need to run the test to ensure everything is good :
    ```
    python -m pytest tests
    ```

    * then run the extraction script
    ```
    cd data_extraction;./data_extraction.py
    ```

2. using the Makefile
The ```Makefile``` is built in a way that the tests will always be ran before the excution. So if something fails, the execution will not start. You just need one command that is:
```
make extract
```

In both cases, you will end up with a DuckDB database named ```project_dw.db```, in the ```data``` folder located in the root of the project folder. This will be our local data warehouse for ingestion process, we will also use it for our staging and dev environment for data transformation.

### Data ingestion

This part is mainly about moving the data from the raw_data schema to the staging schema, and prepare for data transformation. We use aiflow for this.

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

Once airflow is installed, we need first to do two things : 
* create dags and plugins folder into airflow home folder (use ```mkdir```) ;
* change the file ``` airflow.cfg ``` by setting ``` load_examples ``` to false (line 106) and making the dags folder is correctly configured ``` dags_folder = path_to_airflow_home/dags ``` (line 7). The path needs to be absolute (It looks like this in my case ``` dags_folder = ~/de_project/capstone-dezoomcap/airflow/dags ```).

Another point to note is that we will need to add the variable AIRFLOW_HOME to our .profile file, so we don't have to set it manually everytime you launch airflow.

Once airflow is installed and configured, we can now launch it :
``` airflow standalone ```
This command will migrate the database, create a user with admin privileges and launch the scheduler.

2. Running DAGs
In order to run our pipelines, we will create two separate DAGs:
* one that will run only once, and will load all the data from 2019 to 2021, from the raw_data schema to the staging schema ;
* one that will run daily to ingest daily data from 2022.

The scripts can be found in the folder: ``` airflow/dags/ ```.

In order to make sure everything run well, we created a table called ``` dw_log_table ``` in the schema staging. This table actually stores the row counts of all the table within the staging area, after every DAG run, storing the date of of the latest run (in order to simulate or daily import, we started with the first latest date as 31/12/2021, so the daily import will start on 2022/01/01).

If everything went fine, we will get the following in the logs of each tasks:
 
* task batch data - after the load:

![Log shown after the first DAG](../images/logs_first_dag.png)

![First load graphs](../images/first_load_graph.png)

* daily data - example of the log after the first daily load:

![Log shown after the first daily DAG](../images/logs_first_daily.png)

![Daily DAG graphs](../images/daily_dag_graph.png)

#### Troubleshooting
##### Kill all runing airflow subprocess.
```kill -9 `ps -aux | grep airflow | awk '{print $2}'```

### Data transformation

Once dbt is installed, we need to initialise the project, the name of the folder will be ```data_transformation``` in this case.
* initialise the project :
    * check dbt version:  ``` dbt --version```
    this will make sure dbt is correctly istalled ;
    * initialise ```data_transformation``` project: ``` dbt init data_transformation ```
    there will be a prompt asking to select the database used, we will go with duckDB at first, for staging models and dev builds ;
    * enter the newly created folder: ``` cd data_transformation ``` ;
    * create a profile file: ``` touch profiles.yml ``` ;
    * paste the following content inside: 
    ``` 
    data_transformation:
    outputs:
        dev:
        type: duckdb
        path: path/to/you/dckdb/data warehouse/generated in the previous steps.db
        threads: 4

        prod:
        type: duckdb
        path: prod.duckdb
        threads: 4

    target: dev

    ``` 
    For now, we do not care about the values of the prod part, we will change it later ;
    * finaly, add the path (absolute) of profiles.yml the dbt config: 
    ```  
    dbt run --profiles-dir absolute/path/to/data_transformation
    ```
    You should have an ouput like this:
    ```
    15:48:14  Running with dbt=1.7.8
    15:48:15  Registered adapter: duckdb=1.7.2
    15:48:15  Unable to do partial parsing because saved manifest not found. Starting full parse.
    15:48:15  Found 2 models, 4 tests, 0 sources, 0 exposures, 0 metrics, 391 macros, 0 groups, 0 semantic models
    15:48:15  
    15:48:15  Concurrency: 1 threads (target='dev')
    15:48:15  
    15:48:15  1 of 2 START sql table model main.my_first_dbt_model ........................... [RUN]
    15:48:16  1 of 2 OK created sql table model main.my_first_dbt_model ...................... [OK in 0.10s]
    15:48:16  2 of 2 START sql view model main.my_second_dbt_model ........................... [RUN]
    15:48:16  2 of 2 OK created sql view model main.my_second_dbt_model ...................... [OK in 0.03s]
    15:48:16  
    15:48:16  Finished running 1 table model, 1 view model in 0 hours 0 minutes and 0.23 seconds (0.23s).
    15:48:16  
    15:48:16  Completed successfully
    ```
    Now you're good to go. You can follow the doc for further instructions on the [official doc](https://docs.getdbt.com/guides/manual-install?step=1)  of dbt-core.
* create your models (staging, core)
* build the dev environment.

Note that if you do not want to run a dbt command, having to include --profile-dir everytime, you should set an env variable:
```
export DBT_PROFILES_DIR=path/to/directory/containing profiles.yml
``` 
#### Running in production
Once we are finished creating our models locally, and are sure that everything works perfectly, we are ready to deploy to production.
fist, we need to [create the production datawarehouse](https://github.com/drux31/capstone-dezoomcamp/tree/main/production_setup), and then run the following commands (in that order):
1. ``` dbt seed ``` ;
2. ``` dbt run ``` ;
3. ``` dbt test ``` ;
4. ``` dbt build ```.

#### Resources:
- Learn more about dbt [in the docs](https://docs.getdbt.com/docs/introduction)
- Check out [Discourse](https://discourse.getdbt.com/) for commonly asked questions and answers
- Join the [chat](https://community.getdbt.com/) on Slack for live discussions and support
- Find [dbt events](https://events.getdbt.com) near you
- Check out [the blog](https://blog.getdbt.com/) for the latest news on dbt's development and best practices

### Data visualisation

#### Setting up superset
Since we are in dev env, we will just use the docker image to launch superset.

1. download the dockerimage :
```
docker pull apache/superset
```

2. create a secret key:

```
openssl rand -base64 42
```
copy the generated secret key and keep it because we are going to use it later.

3. add bigquery library
The docker image that we pulled contains only the minimal packages needed for testing in dev env. So we will need to add BigQuery manually :

* first we will have to create a docker file
```
touch Dockerfile
```

* file and add our custom in it: 
```
FROM apache/superset
# Switching to root to install the required packages
USER root
# Installing BigQuery dirver to connect to GCP
RUN pip install sqlalchemy-bigquery
# Switching back to using the `superset` user
USER superset
```

* build our custom docker image:
```
docker build -t local/superset . #we add a tag here to specify that the image we will use is different from the one we pulled earlier
```

Now we are ready to launch our image.


4. launch the docker image

```
docker run -d -p 8000:8088 \
-e "SUPERSET_SECRET_KEY=the key generated above" \
--name superset local/superset
```

3. created the admin account
```
docker exec -it superset superset fab create-admin \
--username admin \
--firstname Superset \
--lastname Admin \
--email admin@superset.com \
--password admin
```

4. migrate local db to the latest version 
```
docker exec -it superset superset db upgrade
```

5. load examples (this step is optional) 
```
docker exec -it superset superset load_examples
```

6. setup roles
```
docker exec -it superset superset init
```

and finaly, navigate to http://localhost:8080/login/ and take a look (u/p: [admin/admin])

sources : 
1. [running superset with docker image](https://hub.docker.com/r/apache/superset)
2. [official doc](https://superset.apache.org/docs/intro)