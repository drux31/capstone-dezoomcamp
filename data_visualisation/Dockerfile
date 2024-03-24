FROM apache/superset
# Switching to root to install the required packages
USER root
# Installing BigQuery dirver to connect to GCP
RUN pip install sqlalchemy-bigquery duckdb-engine
# Switching back to using the `superset` user
USER superset