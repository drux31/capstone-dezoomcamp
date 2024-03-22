## Production setup


Since we will be using BigQuery as a production data warehouse, we need to create a corresponding dataset. We will use terraform to do so.
To install terraform, you can refer to the [official doc. ](https://developer.hashicorp.com/terraform/install) - I went for the binary version. 
Once installed, create our ``` main.tf ``` file, and perform our provisionning:
* ``` terraform init ``` ;
* ``` terraform plan ``` &rarr; this step can be skipped, but I liked to do it so I could see what changes will be made before actually appying them ;
* ``` terraform apply ``` ;
* ``` terraform show ``` &rarr; to see the infrastructure that has been created ;
* ``` terraform destroy ``` once we're done.

Note that when setting up terraform with GCP, instead of adding the path to the credentials in the ``` main.tf ``` file, create a local env variable setting the google credentials (terraform wil automatically fetch the credentials from the env path variable) : 
```
export GOOGLE_CREDENTIALS='absolute path to the service account's json credential file'
```

This env variable can be used in profiles.yml, when configuring bigquery, (from dbt) like this:
``` 
keyfile: "{{ env_var('GOOGLE_CREDENTIALS') }}" 
```