# Recieves city string and location tuple (latitude,longitude) and returns json in dict format containing relevant information for further processing
def extraction(city, location):
    lat, long = location
    
    logger.info("Getting information from {}. Latitude and Longitude as stated.".format(city))
    
    url = "https://api.open-meteo.com/v1/forecast?latitude={}&longitude={}&hourly=temperature_2m,relativehumidity_2m,windspeed_10m&windspeed_unit=ms&forecast_days=11&timezone=auto".format(lat, long)
    request_ts = pd.Timestamp.now() 
    response = requests.get(url)
    json = response.json()
    
    # Check for API call errors
    if response.status_code != 200:
        logger.warning("Failed API call for {} city. Reason: '{}'".format(city, json["reason"]))
        return

    #Getting necessary data
    newJson = json["hourly"]
    newJson["city"], newJson["measure_ts"] = city, request_ts

    logger.info("{} city forecast correctly extracted".format(city))
    
    return newJson


# Receives a json containing the correct data, verifies it and then transforms it to return a pandas df
def transformation(json):
    df = pd.DataFrame(json)
    # Verifies data integrity
    if not verification(df): logger.warning("Incorrect data extracted for {} city".format(json["city"])) ; return  

    logger.info("Correct data being processed for {} city".format(json["city"]))

    #Transformations 
    df.rename(columns={"temperature_2m": "temperature_c","windspeed_10m": "windspeed","relativehumidity_2m": "humidity"},inplace=True)
    df = df.iloc[24:]  # Cleaning day 0 temperature 
    df["temperature_c"] = df["temperature_c"].round(2)
    df["temperature_f"] = round(df["temperature_c"] * 1.8 + 32, 2)
    df["temperature_k"] = round(df["temperature_c"] + 273.15, 2)
    df["humidity"] = df["humidity"].round(2)
    df["windspeed"] = df["windspeed"].round(2)

    logger.info("Information processed for {} city".format(json["city"]))
    
    return df


# Connects to database, inserts dataframe and replaces any prediction
def loading(df):
    if df is None: 
        logger.warning("Data was not loaded into table")
        return
    # DDL for local database
    ddl = """ CREATE TABLE IF NOT EXISTS weather_data (
            time datetime64[ns],
            temperature_c float64,
            humidity float64,
            windspeed float64,
            city VARCHAR(255),
            measure_ts datetime64[ns],
            temperature_f float64,
            temperature_k float64,
            unique (city,time, measure_ts)
            ); """
    table = "weather_data"
    db = "weather_database.db"

    if not os.path.isfile(db): logger.info("Database (.db file) was created for the first time")

    # Connects to database, executes ddl and inserts the dataframe 
    conn = sqlite3.connect(db)
    conn.execute(ddl)
    df.to_sql("weather_data", conn, if_exists="append", index=False)
    conn.commit();conn.close()

    logger.info("Database updated with the last API call for {} city".format(df["city"].iloc[0]))


# Checks the data type and looks for null values
def verification(df):
    # Posseses any null value
    if df.isnull().sum().sum() != 0:
        logger.warning("Response json with null values. Failed to process")
        return False

    # Check if the time format is correct
    try:
        df["time"] = pd.to_datetime(df["time"])
    except dateutil.parser._parser.ParserError:
        logger.warning("Response json with incorrect datetime values. Failed to process")
        return False

    expected_types = {
        "time": "datetime64[ns]",
        "temperature_2m": "float64",
        "relativehumidity_2m": "int64",
        "windspeed_10m": "float64",
        "city": "object",
        "measure_ts": "datetime64[ns]",
    }
    
    for col_name, expected_type in expected_types.items():
        if col_name not in df.columns:
            logger.warning(
                "Response json with incorrect column Name. Failed to process"
            )
            return False  # Column not found in DataFrame
        else:
            if df[col_name].dtype.name != expected_type:
                logger.warning("Response json with incorrect data types. Failed to process")
                return False
    return True

# modules used
import sys, requests, logging, pandas as pd, time, sqlite3, os

# logger config
logger = logging.getLogger()
logging.basicConfig(level=logging.INFO)


def main(argv):
    # Gets all cities on specified txt file, citiesLoc by default
    cities =""
    if argv : cities = argv[0]
    else: cities = "citiesLoc.txt"
    
    logger.info("Begginning ETL processes based on cities found in {} text file".format(cities))

    #Iterates through all cities in the txt creating an etl process for each one
    f = open(cities,"r")
    for l in f.readlines():
        city, lat, long = l.strip().split(" ")
        print("="*150)
        logger.info("Begginning ETL process for {} city".format(city))
        print("="*150)
        loading(transformation(extraction(city,(lat,long))))
        print("="*150)
    
    logger.info("Finished ETL")

if __name__ == "__main__":
    main(sys.argv[1:])

