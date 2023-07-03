import json
import requests
import datetime
import pandas as pd
from time import sleep
import io

# Current Datetime (for latest data)
ToSettlementDate = datetime.datetime.utcnow().date()

# Loop to satisfy API rate limits (5-day increments & 10-second sleep in between iterations)
while ToSettlementDate > datetime.datetime.fromtimestamp(1420070400, tz = None).date():
    FromSettlementDate = ToSettlementDate - datetime.timedelta(days=5)
    
    # Query Elexon API
    parameters = {'APIKey' : 'v2fmjr5jvg20irp', 'FromSettlementDate' : FromSettlementDate , 'ToSettlementDate' : ToSettlementDate, 'ServiceType' : 'CSV', }
    response = requests.get('https://api.bmreports.com/BMRS/DERSYSDATA/v1?', params=parameters)
    
    # Parse response, load into DataFrame, export/append to CSV
    entries = pd.read_csv(io.StringIO(response.text), skiprows=1, skipfooter=1, engine='python')
    entries.to_csv('imbalance pricing data.csv', mode='a', index=False, header=False)
    
    # Print latest update
    print("Downloaded data between " + str(FromSettlementDate) + " and " + str(ToSettlementDate))
    
    # Loop requirements
    ToSettlementDate = FromSettlementDate
    sleep(10)