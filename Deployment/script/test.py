import os
import sys
import Get_NY_data
from datetime import datetime
from helper.Storedata import Storedata

if len(sys.argv) >1:
	year = sys.argv[1] #year as input
else:
	year =  current_year = datetime.now().year

storedata = Storedata()

try:
	storedata.create_connection( 'postgres')
except Exception as e:
	print (f'Error creating connection: {e}')

Get_NY_data.fetch_NY_Data(year,storedata)