import pandas as pd
import numpy as np
import sys
import glob
import datetime
import requests
import datetime as dt
import re


#initialize all variables
interesting_cols=[0,1,2,6,7,8,10,12,13,14,15,16,17,19,20,21,22,23,24,25,26,27,28,29,30,48,49,50,60,61,62]     #specify columns to import into dataframes
timestamp=dt.datetime.now().strftime("%Y-%m-%d-%H%M%S")#capture current date and local time represented as string
nxpatterns=['NX2/4/5/6K','NX3K','NX7K','NX9K','SAN']
allrev=0
total=0
ucs=0
nx=0
#Create new dataframe with source of current YTD Bookings Report
df_book = pd.read_csv('c:\\cce\\bookings_source\\fy16.csv',sep=',', engine='python', usecols=interesting_cols)

#Cleanup column nameing
df_book.columns=df_book.columns.str.replace(r'[\(\) \-&]','_')

#Create Dataframe group object pivoting off of Sales Agent Name
g_AM=df_book.groupby('Sales_Agent_Name')

#Create Empty Dataframes for revenue/deal summary data for each AM
df_rev=pd.DataFrame()
df_ucsrev=pd.DataFrame()
df_nxrev=pd.DataFrame()

#Calculate sum of all revenue for each AM and append to new dataframe
for k,group in g_AM:

    total=int(group.BE_CV_Book_Net.sum())
    print (k+':\t\t\t\t  $'+"{:10.2f}".format(total))
    allrev=allrev+total
    rev={'Sales_Agent_Name':k,'Total_Revenue':total}
    df_rev=df_rev.append(rev, ignore_index=True)

#Calculate sum of all UCS Revenue for each AM and append to new dataframe
    ucsgroup=group[group.Internal_Business_Entity_Description =='Computing Systems']
    ucstotal=int(ucsgroup.BE_CV_Book_Net.sum())
    ucs=ucs+ucstotal
    ucsrev={'Sales_Agent_Name':k,'UCS_Revenue':ucstotal}
    df_ucsrev=df_ucsrev.append(ucsrev, ignore_index=True)


    nxgroup=group[group.Internal_Sub_Business_Entity_Description.isin(nxpatterns)]
    nxtotal=int(nxgroup.BE_CV_Book_Net.sum())
    nx=nx+nxtotal
    nxrev={'Sales_Agent_Name':k,'NX_Revenue':nxtotal}
    df_nxrev=df_nxrev.append(nxrev, ignore_index=True)

#Reindex df on agent name
df_rev=df_rev.set_index('Sales_Agent_Name')
df_ucsrev=df_ucsrev.set_index('Sales_Agent_Name')
df_nxrev=df_nxrev.set_index('Sales_Agent_Name')

#combine dfs for final calculations
df_final=pd.concat([df_rev,df_ucsrev,df_nxrev], axis=1, join_axes=[df_rev.index])
df_final['UCS_Pen_Rate']= (df_final.UCS_Revenue / df_final.Total_Revenue)
df_final['NX_Pen_Rate']=(df_final.NX_Revenue / df_final.Total_Revenue)
print(df_final)



df_final.to_csv("c:/cce/Output/Bookings_"+timestamp+".csv", index=True)  #write  dataframe to csv file

'''df_savm.columns=['SAVID','Company_Name','Created Date','AM','AM_Team','Vertical','Address','City','ZIP','State','GUID','GU_Name']


'''
