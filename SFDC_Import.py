import pandas as pd
import os
import sys
import glob
import datetime as dt
import re

from pandas import ExcelWriter

timestamp=dt.datetime.now().strftime("%Y-%m-%d-%H%M%S")#capture current date and local time represented as string
sfdc_file='c:\\cce\\SFDC_Source\\SFDC.xlsx'  #SFDC Contacts to Export
#dictionary of full names translated to cec id
amtouid={'JOSHUA BAILEY':'josbaile','JAMES KNIGHT': 'jameknig', 'DAVID OWEN':'daowen',
         'ERIC HESTON':'erheston', 'PATRICK JOHNSON':'patrijoh', 'MARCUS KIRBY':'makirby',
         'CHRISTOPHER LITSTER':'chlitst', 'PATRICK PAULUZZI':'ppauluzz',
         'ANNE-MARIE FIGARD':'aknowlto', 'GENE GALIN':'egalin'}

#list of all my ams
myams=['josbaile', 'jameknig', 'daowen', 'erheston', 'patrijoh', 'makirby', 'chlitst', 'ppauluzz', 'aknowlto', 'egalin']
emailpattern='[A-Za-z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}'
statelist=['OK','AR','LA','MS','AL']

#list of executive titles
executives='(C.O)|(CHIEF).*|(PRES).*'
directors='(DIRECTOR).*'
techies='(ENGINEER).*|(IT ).*|(INFORMA).*|(TECHNO).*|(NETWORK).*'




#open source sfdc excel file
df_sfdc=pd.read_excel(sfdc_file, sheetname='SFDC Contact Export 110915', header=[0], na_values="")

#remove spaces and slashes from column names
df_sfdc.columns=df_sfdc.columns.str.replace('[ /]','_')

#remove unnecessary columns
df_sfdc.drop(df_sfdc.columns[[1,5,6,7,8,9,10,11,12,14,15,16,18,19,22,23]], axis=1, inplace=True)

#Convert account owner name to am's cec id
df_sfdc['Account_Owner'].replace(amtouid,inplace=True)

#Rename Columns
df_sfdc.rename(columns = {'Account_Owner':'AM','State_Province':'State','Account_Name':'Company_Name'}, inplace = True)

#remove contacts that do not belong to my ams
df_sfdc=df_sfdc[df_sfdc.AM.isin(myams)]
df_sfdc.Title=df_sfdc.Title.str.upper()

#df_sfdc.index=range(len(df_sfdc.index)) #reindex



#Remove rows with empty critical columns (First Name, email address)
df_sfdc=df_sfdc[df_sfdc.Email.notnull()]  #Keep only customer rows with email not blank
df_sfdc=df_sfdc[df_sfdc.First_Name.notnull()]  #remove rows with no first name

#Clean Customer data without removing rows
df_sfdc.Email=df_sfdc.Email.str.lower().str.strip() #strip whitespace and apply lower case
df_sfdc['Email'].replace(to_replace='[^a-zA-Z0-9%_@+-.]+',value='',inplace=True,regex=True) #remove invalid characters

#Remove rows with invalid data (email, and other AMs)
df_sfdc=df_sfdc[df_sfdc.Email.str.match(emailpattern, as_indexer=False)]#remove rows with invalid email patterns

#Remove Duplicate Emails

df_sfdc.drop_duplicates({'Email'}, keep='first', inplace=True)

#Clean secondary data
df_sfdc.First_Name=df_sfdc.First_Name.str.title().str.strip().str.replace('[^a-zA-Z ]+','')#clean first name
df_sfdc.Last_Name=df_sfdc.Last_Name.str.title().str.strip().str.replace('[^a-zA-Z ]+','')#clean last name
#df_sfdc.Company_Name=df_sfdc.Company_Name.str.replace('"','')
df_sfdc.City=df_sfdc.City.str.title().str.strip()
df_sfdc.State=df_sfdc.State.str.replace(r'[oO][kK].+','OK')
df_sfdc.State=df_sfdc.State.str.replace(r'[lL][oO].+','LA')
df_sfdc.State=df_sfdc.State.str.replace(r'[aA][lL].+','AL')
df_sfdc.State=df_sfdc.State.str.replace(r'[mM][iI].+','MS')
df_sfdc.State=df_sfdc.State.str.replace(r'[aA][rR].+','AR')
df_sfdc.State=df_sfdc.State.str.upper()
df_sfdc=df_sfdc[df_sfdc.State.notnull()]
df_sfdc=df_sfdc[df_sfdc.State.isin(statelist)] #remove rows if customer state is not in territory
df_sfdc['Source']='SFDC'
df_exec=df_sfdc[df_sfdc.Title.str.contains(executives)==True]
df_dir=df_sfdc[df_sfdc.Title.str.contains(directors)==True]
df_techies=df_sfdc[df_sfdc.Title.str.contains(techies)==True]
df_sfdc.to_csv("c:/cce/Output/Customers_SFDC_"+timestamp+".csv", index=False)  #write  dataframe to csv file
df_techies.to_csv("c:/cce/Output/Customers_SFDC_Techies_"+timestamp+".csv", index=False)  #write  dataframe to csv file
df_dir.to_csv("c:/cce/Output/Customers_SFDC_Dirs_"+timestamp+".csv", index=False)  #write  dataframe to csv file
df_exec.to_csv("c:/cce/Output/Customers_SFDC_Execs_"+timestamp+".csv", index=False)  #write  dataframe to csv file

