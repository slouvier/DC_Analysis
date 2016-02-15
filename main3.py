import pandas as pd  #pandas library for data analysis objects
import os               #os library for file system access
import glob
import datetime as dt   #datetime library for timestamping of files
import re               #regular expression library for RE comparisons of patterns

from pandas import ExcelWriter


from pandas import ExcelFile

#setup all variables for file handling
df_list_cust=[]     #arrays for dataframe objects
df_list_partner=[]
interesting_cols=[0,6,31,32,34,39,40,41,42]     #specify columns to import into dataframes
partner_int_cols=[0,6,9,16,21,27,28,29,31,32,34,42,53,69]
timestamp=dt.datetime.now().strftime("%Y-%m-%d-%H%M%S")#capture current date and local time represented as string

#Regular Expressions & Variables
emailpattern='[A-Za-z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}'
dealidpattern='[0-9]{7,8}'
datepattern='\d{2}-[a-zA-Z]{3}-\d{2}'
statepattern='[ALOM][ARSKL]'
myams=['josbaile', 'jameknig', 'daowen', 'erheston', 'patrijoh', 'makirby', 'chlitst', 'ppauluzz', 'aknowlto', 'egalin']
statelist=['OK','AR','LA','MS','AL']
domain_pattern='.+@'

'''NOTES:  pd.read_csv cannot import parsed dates with inconsistencies.  Therefore created a custom date parser
to evaluate with date_parser=parser and compare against a regular expression meeting the date format provided by CCW.
In the event of no pattern matching, the parser returns a value of ''.
'''
redate=re.compile(datepattern)
parser = lambda date: dt.datetime.strptime(date, '%d-%b-%y') if redate.match(date) else ''

#for fname in glob.glob('c:\\cce\\Output\\*.csv'):
#    os.remove(fname)

for fname in glob.glob('c:\\cce\\Output\\Analysis\\*.*'):
    os.remove(fname)

#loop through each .xls file in the c:\cce directory, import into a dataframe, and add dataframe to the array
for i,fname in enumerate(glob.glob('c:\\cce\\*.xls')):
    print(str(i)+' '+fname)
    df_list_cust.insert(i,pd.read_csv(fname, sep=r'\t', usecols=interesting_cols, engine='python', parse_dates=[1], date_parser=parser))
    df_list_partner.insert(i, pd.read_csv(fname, sep=r'\t', usecols=partner_int_cols, engine='python', parse_dates=[1],date_parser=parser))

#Concatenate individual dataframes into a master dataframe & rename columns
df_cust = pd.concat(df_list_cust,ignore_index=True)   #Combine individual dataframes in df_list
df_partner = pd.concat(df_list_partner,ignore_index=True)
#df_cust.index=range(len(df_cust.index))
#modify dataframe column names and dtypes
df_cust.columns=['Deal_ID','Created_Date','Company_Name', 'City','State','First_Name','Last_Name','Email','AM']
df_cust.Deal_ID=df_cust.Deal_ID.astype(str)

df_partner.columns=['Deal_ID','Created_Date','Deal_Status','Partner_Company','Partner_City',
                          'Partner_First_Name','Partner_Last_Name','Partner_Email','Customer_Company',
                          'Customer_City','Customer_State','Cisco_AM','Technology','Quote_ID']

df_partner.Deal_ID=df_partner.Deal_ID.astype(str)

#Customer dataframe cleanup
df_cust=df_cust[df_cust.Deal_ID.str.match(dealidpattern)]#remove rows with invalid dealid patterns

#Remove rows with empty critical columns (First Name, email address)
df_cust=df_cust[df_cust.Email.notnull()]  #Keep only customer rows with email not blank
df_cust=df_cust[df_cust.First_Name.notnull()]  #remove rows with no first name

#Clean Customer data without removing rows
df_cust.Email=df_cust.Email.str.lower().str.strip() #strip whitespace and apply lower case
df_cust['Email'].replace(to_replace='[^a-zA-Z0-9%_@+-.]+',value='',inplace=True,regex=True) #remove invalid characters

#Remove rows with invalid data (Email, and other AMs)
df_cust=df_cust[df_cust.Email.str.match(emailpattern, as_indexer=False)]#remove rows with invalid email patterns
df_cust=df_cust[df_cust.AM.isin(myams)]#remove rows with AMs other than myams

#Remove Duplicate Emails

df_cust=df_cust.sort_values(['Email','Created_Date'],ascending=[True, False])
df_cust.drop_duplicates({'Email'}, keep='first', inplace=True)

#Clean secondary data
df_cust.First_Name=df_cust.First_Name.str.title().str.strip().str.replace('[^a-zA-Z ]+','')#clean first name
df_cust.Last_Name=df_cust.Last_Name.str.title().str.strip().str.replace('[^a-zA-Z ]+','')#clean last name
df_cust.Company_Name=df_cust.Company_Name.str.replace('"','')
df_cust.City=df_cust.City.str.title().str.strip()
df_cust.State=df_cust.State.str.replace(r'[oO][kK].+','OK')
df_cust.State=df_cust.State.str.replace(r'[lL][oO].+','LA')
df_cust.State=df_cust.State.str.replace(r'[aA][lL].+','AL')
df_cust.State=df_cust.State.str.replace(r'[mM][iI].+','MS')
df_cust.State=df_cust.State.str.replace(r'[aA][rR].+','AR')
df_cust.State=df_cust.State.str.upper()
df_cust=df_cust[df_cust.State.notnull()]
df_cust=df_cust[df_cust.State.isin(statelist)] #remove rows if customer state is not in territory
#df_cust=df_cust[df_cust.State.str.match(statepattern)] #remove rows with customers in states other than LA, MS, AL, AR, OK
df_cust['Source']='CCW'

#Drop duplicate deals & pre-clean

df_partner=df_partner[df_partner.Deal_ID.str.match(dealidpattern)]#remove rows with invalid dealid patterns indicating bad or misaligned data

#Convert errored states with full name to two letter state abbr
df_partner.Customer_State=df_partner.Customer_State.str.replace(r'[oO][kK].+','OK')\
    .str.replace(r'[lL][oO].+','LA').str.replace(r'[aA][lL].+','AL').str.replace(r'[mM][iI].+','MS')\
    .str.replace(r'[aA][rR].+','AR').str.upper()

df_partner=df_partner[df_partner.Customer_State.isin(statelist)] #remove rows if customer state is not in territory
df_partner.drop_duplicates({'Deal_ID'}, keep='first',inplace=True) #remove duplicate rows by deal id
df_partner.Partner_Company=df_partner.Partner_Company.str.replace('"','') #delete floating quotes
df_partner=df_partner[df_partner.Cisco_AM.isin(myams)]#remove rows with AMs other than myams
df_partner.Partner_First_Name=df_partner.Partner_First_Name.str.title().str.strip().str.replace('[^a-zA-Z ]','')#clean first name
df_partner.Partner_Last_Name=df_partner.Partner_Last_Name.str.title().str.strip().str.replace('[^a-zA-Z ]','')#clean last name
df_partner.Partner_Email=df_partner.Partner_Email.str.lower().str.strip().str.replace(' ','')#clean email
df_partner.Customer_Company=df_partner.Customer_Company.str.replace('"','')
df_partner.Technology=df_partner.Technology.str.replace('"','')
df_partner.Partner_City=df_partner.Partner_City.str.title().str.strip() #clean Partner city#df_partner=df_partner[df_partner.Deal_ID.notnull()] #remove rows with no dealid

df_deals=df_partner.copy(deep=True)  #copy df_partner into new df object with all deals for deal level analysis

#Remove rows not needed for unique partner contacts
df_partner=df_partner[df_partner.Partner_Email.str.match(emailpattern)==True]#remove rows with invalid email patterns
df_partner=df_partner[df_partner.Partner_Email.notnull()] #remove rows with blank email addresses
df_partner=df_partner[df_partner.Partner_First_Name.notnull()] #remove rows with no partner first name
df_partner=df_partner.sort_values(['Partner_Email','Created_Date'],ascending=[True, False])  #sort rows by Partner email address & created date
df_partner=df_partner.drop_duplicates({'Partner_Email'}, keep='first') #remove duplicates by email but keep the most recent entry
df_partner.sort_values(['Created_Date'],ascending=[False], inplace=True) #Sort rows by Created Date

#Find all unique partner email domains and remove duplicates
partner_domains = pd.Series(df_partner.Partner_Email,name='domain')
partner_domains= partner_domains.str.replace(domain_pattern,'@')
partner_domains.drop_duplicates(inplace=True)
partds=partner_domains.values.tolist()
df_cust=df_cust[~df_cust.Email.str.replace(domain_pattern,'@').isin(partds)] #Remove customer contact rows that have a partner domain name in the customer email column

'''
df_partner.Customer_State=df_partner.Customer_State.str.replace(r'[lL][oO].+','LA')
df_partner.Customer_State=df_partner.Customer_State.str.replace(r'[aA][lL].+','AL')
df_partner.Customer_State=df_partner.Customer_State.str.replace(r'[mM][iI].+','MS')
df_partner.Customer_State=df_partner.Customer_State.str.replace(r'[aA][rR].+','AR')
df_partner.Customer_State=df_partner.Customer_State.str.upper()
'''


#Write Clean data to individual CSVs for import into email distribution tools
df_cust.to_csv("c:/cce/Output/Customers_"+timestamp+".csv", index=False)  #write  dataframe to csv file
df_cust[df_cust.State=='OK'].to_csv("c:/cce/Output/Customers_OK_"+timestamp+".csv", index=False)  #write  dataframe to csv file
df_cust[df_cust.State=='LA'].to_csv("c:/cce/Output/Customers_LA_"+timestamp+".csv", index=False)  #write  dataframe to csv file
df_cust[df_cust.State=='AR'].to_csv("c:/cce/Output/Customers_AR_"+timestamp+".csv", index=False)  #write  dataframe to csv file
df_cust[df_cust.State=='MS'].to_csv("c:/cce/Output/Customers_MS_"+timestamp+".csv", index=False)  #write  dataframe to csv file
df_cust[df_cust.State=='AL'].to_csv("c:/cce/Output/Customers_AL_"+timestamp+".csv", index=False)  #write  dataframe to csv file
df_partner.to_csv("c:/cce/Output/Partners_"+timestamp+".csv", index=False)
df_partner[df_partner.Customer_State=='OK'].to_csv("c:/cce/Output/Partners_OK_"+timestamp+".csv", index=False)  #write  dataframe to csv file
df_partner[df_partner.Customer_State=='AR'].to_csv("c:/cce/Output/Partners_AR_"+timestamp+".csv", index=False)  #write  dataframe to csv file
df_partner[df_partner.Customer_State=='LA'].to_csv("c:/cce/Output/Partners_LA_"+timestamp+".csv", index=False)  #write  dataframe to csv file
df_partner[df_partner.Customer_State=='MS'].to_csv("c:/cce/Output/Partners_MS_"+timestamp+".csv", index=False)  #write  dataframe to csv file
df_partner[df_partner.Customer_State=='AL'].to_csv("c:/cce/Output/Partners_AL_"+timestamp+".csv", index=False)  #write  dataframe to csv file

#Combine into single XLS
writer = ExcelWriter("c:/cce/Output/Analysis/Analysis_"+timestamp+'.xlsx')
df_cust.to_excel(writer,'Customer Contacts',index=False)
df_partner.to_excel(writer,'Partner Contacts', index=False)
df_deals.to_excel(writer,'Partner Deals', index=False)
writer.save()



# #Dump file of current customer df
#df_cust.to_csv("c:/cce/Dump_Cust_"+timestamp+".csv", index=True)  #write  dataframe to csv file

'''****************Phase 2 - Actionable data
Find unique partners and group data by partner
   Number of deals by daterange
   Number of deals with Computing Systems
   % of deals with Computing Systems Technology

'''
#Experimenting with groupby objects for data analysis below

df_deals_dc=df_deals.copy(deep=True)    #make a copy of the deals dataframe
df_deals_dc=df_deals_dc.drop_duplicates({'Deal_ID'}, keep='first')      #Drop duplicates based on Deal ID
df_deals_dc=df_deals_dc[df_deals_dc.Created_Date > '7/28/2015']         #drop all deals except for this fiscal year
df_deals_dc=df_deals_dc[df_deals_dc.Technology.str.contains('COMPUTING|NX',na=False)]
#df_deals_dc.to_csv("c:/cce/Dump_DCDeals_"+timestamp+".csv", index=True)  #write  dataframe to csv file
#print (df_deals_dc)

gdeal_all = df_deals.groupby('Partner_Company')
gdeal_dc=df_deals_dc.groupby('Partner_Company')
df_summary_all=pd.DataFrame(gdeal_all.Customer_State.apply(pd.value_counts).unstack(-1).fillna(0))
df_summary_all['Total_Deal_Count']=df_summary_all.sum(axis=1)
df_summary_all.sort_values(by="Total_Deal_Count",ascending=False,inplace=True)

df_summary_dc=pd.DataFrame(gdeal_dc.Customer_State.apply(pd.value_counts).unstack(-1).fillna(0))
df_summary_dc['Total_Deal_Count']=df_summary_dc.sum(axis=1)
df_summary_dc.sort_values(by="Total_Deal_Count",ascending=False,inplace=True)

df_summary_dc.to_csv("c:/cce/Output/Analysis/Deal_Analysis_"+timestamp+".csv", index=True)  #write  dataframe to csv file
