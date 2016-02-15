import pandas as pd
import numpy as np
import sys
import glob
import datetime
import requests
import datetime as dt
import re
import simplekml

from pandas import ExcelWriter
from pandas import ExcelFile

#setup all variables for file handling
interesting_cols=[0,3,6,14,15,29,49,51,52,54,56,57]     #specify columns to import into dataframes
datepattern='\d{2}-[a-zA-Z]{3}-\d{2}'
redate=re.compile(datepattern)
parser = lambda date: dt.datetime.strptime(date, '%d-%b-%y') if redate.match(date) else ''
google_maps_api_key = 'AIzaSyBiWvlB5pjXhgaKnTFMUhK5hX4MaCtBsIg'
timestamp=dt.datetime.now().strftime("%Y-%m-%d-%H%M%S")#capture current date and local time represented as string

#Sample KML
kml = simplekml.Kml()

#loop through each .csv file in the c:\cce directory, import into a dataframe
df_savm = pd.read_csv('c:\\cce\\savm_source\\savm.csv',sep=',', engine='python', usecols=interesting_cols, parse_dates=[2], date_parser=parser)

df_savm.columns=['SAVID','Company_Name','Created Date','AM','AM_Team','Vertical','Address','City','ZIP','State','GUID','GU_Name']

df_savm.drop_duplicates({'SAVID'}, keep='first',inplace=True)
df_savm['AM']= np.where(df_savm['AM_Team'].isnull(), df_savm['AM'], df_savm['AM_Team'])

print(df_savm.AM.count())
HC = df_savm[df_savm.Vertical=='Health Care']
print(df_savm.AM.value_counts())
print(HC.AM.value_counts())
g_am=df_savm.groupby('AM').count()
print(g_am)

def geo(x):
    try:
        if x=='': return (0,0)
        url ='https://maps.googleapis.com/maps/api/geocode/json?address='+str(x)+'&key='+google_maps_api_key
        r = requests.get(url).json()
        lat = r['results'][0]['geometry']['location']['lat']
        lng = r['results'][0]['geometry']['location']['lng']
        print ('Location:  '+x+'   Lat:  '+str(lat)+'   Lng:  '+str(lng))
        return (lat,lng)
    except:
        print(str(x)+' Failed')
        return (0,0)
x=np.vectorize(geo,otypes=[np.float,np.float])
df_savm['Lat'],df_savm['Lng']=(x(df_savm.Address+','+df_savm.City+','+df_savm.State))
df_savm.to_csv("c:/cce/Output/GeoSAVM_"+timestamp+".csv", index=False)  #write  dataframe to csv file
'''
def kmlconv(Acct, AM,Lat, Lng):

    global kml
    kml.newpoint(name=Acct, coords=[(Lng,Lat)])

   # kml.save("c:\\cce\\botanicalgarden.kml")

y=np.vectorize(kmlconv)
y(df_savm.Company_Name,df_savm.AM, df_savm.Lat, df_savm.Lng)
kml.save("c:\\cce\\SAVM_Map.kml")

grouped_state_df = df_addresses.groupby('State')
print (grouped_state_df)

# set your google maps api key here.

# get the list of states from our DataFrame.
for state in states:
    # make request to google_maps api and store as json. pass in the geo name to the address
    # query string parameter.

    # Get lat and long from response. "location" contains the geocoded lat/long value.
    # For normal address lookups, this field is typically the most important.
    # https://developers.google.com/maps/documentation/geocoding/#JSON

    lat = r['results'][0]['geometry']['location']['lat']
    lng = r['results'][0]['geometry']['location']['lng']
    print ('State:  '+state+'   Lat:  '+str(lat)+'   Lon:  '+str(lng))
'''