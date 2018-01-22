# -*- coding: utf-8 -*-
"""
Spyder Editor

This is a temporary script file.
"""

import simplejson,json
import requests
from datetime import date,timedelta

baseurl ="http://aviationweather.ncep.noaa.gov/gis/scripts/AirSigmetJSON.php?"
datevariable="date="
refdate="20171219"

def getDayData(retrievaldate, baseurl = baseurl, datevariable=datevariable):
    date_string=retrievaldate.strftime('%Y%m%d')
    url = baseurl+datevariable+date_string
    resp = requests.get(url=url)
    data = json.loads(resp.text)
    return data

current = date.today()
daybefore = current - timedelta(1)
output_data = None
while current.year>2016:
    temp = getDayData(current)
    with open(current.strftime('%Y%m%d')+'.json', 'w', encoding='utf8') as f:
        simplejson.dump(temp,f)
    if output_data == None:
        output_data = temp
    else:
        output_data.get("features").extend(temp.get("features"))
    print(current)
    current = current - timedelta(1)
    
#iwith open('complete.json', 'w', encoding='utf8') as f:
#    simplejson.dump(output_data,f)
