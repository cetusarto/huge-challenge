
# Creates dataset
def create_dataset():
    dataset = bigquery.Dataset(dataset_id)
    dataset.location = "US"
    dataset = client.create_dataset(dataset, timeout=30)
    print("Created dataset {}.{}".format(client.project, dataset.dataset_id))

# Checks dataset existance
def dataset_exists():
    try:
        client.get_dataset(dataset_id)
        print("Dataset {} already exists".format(dataset_id))
        return True
    except NotFound:
        print("Dataset {} is not found".format(dataset_id))
        return False

# Modules used
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
import pandas_gbq, pandas as pd, sqlite3,sys

# Global variables for Client server connection
client = bigquery.Client()
dataset = "weather"
table = "predictions"
dataset_id= "{}.{}".format(client.project,dataset)
table_id = "{}.{}.{}".format(client.project,dataset,table)


def main(args):
    if not dataset_exists(): create_dataset()

    # Gets temperature unit for BQ query. C is Celsius, K is Kelvin and F is Fahrenheit
    temp = False
    if args : temp =args[0] 

    # Connect to sqlite db to get the local data
    conn = sqlite3.connect('weather_database.db')
    df = pd.read_sql_query("SELECT * FROM weather_data", conn)
    df["time"] = pd.to_datetime(df["time"])
    df["measure_ts"] = pd.to_datetime(df["measure_ts"])

    # upload DF to bq
    pandas_gbq.to_gbq(df, "{}.{}".format(dataset,table), project_id=client.project, if_exists='append')
    print("Uploaded to BQ")
    
    conn.execute("DELETE FROM weather_data where True")
    print("Deleted from local database")

    # If specified argument, does the query
    if temp:
        if temp not in ['c','k','f']: print("Incorrect parameter", temp,". It should be c, k or f"); conn.commit();conn.close(); return
        print("Querying and printing query using", temp, "as units")
        sql = f"""
                WITH CTE AS ( (
                    SELECT city, CAST(time AS DATE) AS day, temperature_{temp} as temp
                    FROM {table_id} a WHERE
                    a.measure_ts = (
                        SELECT MAX(b.measure_ts)
                        FROM {table_id} b
                        WHERE b.city = a.city AND b.time = a.time) ) )
                SELECT city, DAY, AVG(temp) AS avg_temp, MAX(temp) AS min_temp, MIN(temp) AS max_temp
                FROM CTE GROUP BY CITY, DAY ; 
         """
        print("Querying default query to bq, expecting results")
        query_job = client.query(sql)
        data = query_job.result()
        rows = data.total_rows
        print("Rows recieved:", rows)

    conn.commit();conn.close()
    
if __name__ == "__main__":
    main(sys.argv[1:])


