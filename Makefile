# Running the data extraction
extract: test_extract
	cd data_extraction;./data_extraction.py

test_extract:
	python -m pytest tests

### Automting git commit and push process
## parameter : comment - for the commit
git_push: git_commit
	git push -u origin main

git_commit: git_add
	git commit -m "$(comment)"
git_add:
	git add .

### Loading airflow
load_airflow: set_env
	airflow standalone
set_env:
	export AIRFLOW_HOME=~/de_project/capstone-dezoomcap/airflow/