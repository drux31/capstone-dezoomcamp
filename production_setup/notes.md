when setting up terraform with GCP, instead of adding the path to the credentials in the file, create a local env variable setting the google credentials (terraform wil automatically fetch the credentials from the env path variable) : 
	```
	export GOOGLE_CREDENTIALS='absolute path to the service account's json credential file'
	```

export GOOGLE_APPLICATION_CREDENTIALS=~/.google/credentials/drux-de-zoomcamp-terraform-runner.json
pip install --upgrade google-cloud-bigquery
