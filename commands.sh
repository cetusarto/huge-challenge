#! /bin/bash
sudo apt install git-all
sudo apt update
sudo apt install python3 
PROJECT_ID = $(gcloud config get project)
gcloud config set project $PROJECT_ID
pip install requests
pip install pandas
pip install google-cloud-bigquery
pip install pandas-gbq -U
pip install sqlite3
