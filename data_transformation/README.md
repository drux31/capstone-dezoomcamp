## Data transformation

For this part, we will be working with dbt core. The transformation is divided in three parts : 
1. creating the models for the staging area ;
2. creating the core models for the dev builds ;
3. building the models for production (into BigQuery).

Being concrete, we will mainly have to do following :
* install dbt-cloud (using pip - ``` python -m pip install dbt-core ```) ; 
* install dbt-ducdb package, to be able work with duckdb (``` python -m pip install dbt-duckdb ```);
* install dbt-bigquery to be able to work with BigQuery (``` python -m pip install dbt-bigquery ```);
* build the model for production ;
* schedule the building with airflow, since we're working locally with dbt-core.

### Creating the models for the staging area

#### Staging models
Our staging area includes the following models : 
* ```stg_caracteritiques```: which transforms the data about the characteristics of all the accidents (identified uniquely) ;
* ```stg_lieux```: which transforms the data about the location of the accidents ;
* ```stg_usagers```: which transforms the data about the people involved in every accidents ;
* ```stg_vehicules```: which transforms the data about the vehicules involved in every accidents.

we also included some test in the models, so that everytime we run, we make sure that nothing is broken.

#### Seeds
Seeds are CSV files that are loaded directly into the data warehouse, to serve as a dimension for the final model. We did add three seeds in our models.

#### Macros
Macros are Jinja pieces of code that can be reused multiple times. We defined some macros in our project, that we use to avoid repeating some portion of codes several times.

#### Core models



### Tips for the local installation

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

### Using the starter project (official doc)

Try running the following commands:
- dbt run
- dbt test


### Resources:
- Learn more about dbt [in the docs](https://docs.getdbt.com/docs/introduction)
- Check out [Discourse](https://discourse.getdbt.com/) for commonly asked questions and answers
- Join the [chat](https://community.getdbt.com/) on Slack for live discussions and support
- Find [dbt events](https://events.getdbt.com) near you
- Check out [the blog](https://blog.getdbt.com/) for the latest news on dbt's development and best practices
