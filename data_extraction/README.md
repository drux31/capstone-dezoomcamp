## Data extraction
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
    cd data_extraction;./data_extraction.py
    ```

2. using the Makefile
The ```Makefile``` is built in a way that the tests will always be ran before the excution. So if something fails, the execution will not start. You just need one command that is:
```
make extract
```

In both cases, you will end up with a DuckDB database named ```project_dw.db```, in the ```data``` folder located in the root of the project folder. This will be our local data warehouse for ingestion process, we will also use it for our staging and dev environment for data transformation.

### Architecture
![alt Architexture for data extraction](../images/data_extraction.png)