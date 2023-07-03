import json
import requests
import datetime
import pandas as pd
from time import sleep

# Current Datetime (for latest data)
datetimeto = datetime.datetime.utcnow()

# Loop to satisfy API rate limits (5-day increments & 10-second sleep in between iterations)
while datetimeto > datetime.datetime.fromtimestamp(1420070400, tz = None):
    datetimefrom = datetimeto - datetime.timedelta(days=5)

    # Query Elexon API
    parameters = {'from' : datetimefrom, 'to' : datetimeto}
    response = requests.get('https://data.elexon.co.uk/bmrs/api/v1/forecast/generation/wind/latest?', params=parameters)
    
    # Parse JSON response, load into DataFrame
    payload = response.json()
    flatten = payload['data']
    entries = pd.DataFrame(flatten)
    
    # Write/Append to CSV
    entries.to_csv('wind_gen_forecast_data.csv', mode='a', index=False, header=False)
    
    # Print latest update
    print("Downloaded data between " + str(datetimefrom) + " and " + str(datetimeto))
    
    # Loop requirements
    datetimeto = datetimefrom
    sleep(10)