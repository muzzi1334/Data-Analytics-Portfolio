import json
import requests
import datetime
import pandas as pd
from time import sleep

# Current Date (for latest data)
dateto = datetime.datetime.utcnow().date()

# Loop to satisfy API rate limits (5-day increments & 10-second sleep in between iterations)
while dateto > datetime.datetime.fromtimestamp(1420070400, tz = None).date():
    datefrom = dateto - datetime.timedelta(days=5)

    # Query Elexon API
    parameters = {'settlementDateFrom' : datefrom, 'settlementDateTo' : dateto, 'fuelType' : 'WIND'}
    response = requests.get('https://data.elexon.co.uk/bmrs/api/v1/datasets/FUELHH?', params=parameters)
    
    # Parse JSON response, load into DataFrame
    payload = response.json()
    flatten = payload['data']
    entries = pd.DataFrame(flatten)
    
    # Write/Append to CSV
    entries.to_csv('wind_gen_outturn_data.csv', mode='a', index=False, header=False)
    
    # Print latest update
    print("Downloaded data between " + str(datefrom) + " and " + str(dateto))
    
    # Loop requirements
    dateto = datefrom
    sleep(10)