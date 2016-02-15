# DC_Analysis
PSS DC Analysis Tools
This repository includes a number of python/pandas scripts for data analysis of raw data content accessible to PSSs
from multiple sources.  Each script is given a the objective of converting, cleaning or merging the raw data into a
format that is either usable by another system, valuable for providing to internal teams, or provides additional
insights on historical performance.  Below is a description of the functionality that each script serves.

-SAVM_4_Avention.py - converts the raw SAVM csv data to individual csv files for each AM/VSAM assignment.  Also dedupes
data by SAVM ID and adds colums for use in importing accounts into Avention

-main3.py - Combines individual CCW exports into a single dataframe, deduplicates and cleans all fields and exports
into a combined and individual list of contacts by AM.  Does the same function for extracting partner contact information
for all deals registered to territory accounts.

-SAVM_Geotagging.py - deduplicates raw SAVM account information and uses Google API to download LAT/LON information for
all accounts so they may be viewed in google earth. (Work in Progress)

-fy16bookings.py - begins with raw YTD bookings report and performs a variety of analyses including revenue by subbe,
penetration rates, dealcount by am, deals by vertical, revenue by vertical, etc. (Work in progress)

-SFDC_Import - cleans contact information for exported SFDC contacts (Work in Progress)