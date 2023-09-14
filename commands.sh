#! /bin/bash
<<<<<<< HEAD
sudo apt install git
sudo apt update
sudo apt install python3 
=======
>>>>>>> 78604c4cbb22b96fc74102b28f3f98db3baaf9f0
PROJECT_ID = $(gcloud config get project)
gcloud config set project $PROJECT_ID
pip install requests
pip install pandas
pip install google-cloud-bigquery
pip install pandas-gbq -U
pip install sqlite3
