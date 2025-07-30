#!/bin/bash

python3 -m venv env
source env/bin/activate
pip install --upgrade pip 

pip install dbt-core==1.9.4
pip install dbt-athena==1.9.4
pip install dbt-utils
# pip install dbt-core==1.9.4
# pip install dbt-glue==1.9.4
# pip install dbt-athena==1.9.4
# dbt init dbt_demo_acdc


mkdir -p dbt_demo_acdc/models/staging
mkdir -p dbt_demo_acdc/models/marts
mkdir -p infrastructure/lambda_functions
mkdir -p infrastructure/step_functions
mkdir -p test_data
mkdir -p docs

touch dbt_demo_acdc/profiles.yml
touch infrastructure/requirements.txt

source env/bin/activate
cd dbt_demo_acdc
dbt init
dbt deps