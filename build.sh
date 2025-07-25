#!/bin/bash

python3 -m venv env
source env/bin/activate
pip install dbt-core dbt-glue
dbt init dbt_project


mkdir -p dbt_project/models/staging
mkdir -p dbt_project/models/marts
mkdir -p infrastructure/lambda_functions
mkdir -p infrastructure/step_functions
mkdir -p test_data
mkdir -p docs

touch dbt_project/profiles.yml
touch infrastructure/requirements.txt