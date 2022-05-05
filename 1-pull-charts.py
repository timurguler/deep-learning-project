'''
PROCEDURE 1 - SCRAPING CHARTS FROM BILLBOARD - CHANGE URL AND AWS FOLDER NAME BASED ON COUNTRY/POP

TIMUR GULER
04.27.2022
'''

##########
# STEP 0 - IMPORT NECESSARY PACKAGES AND SET UP AWS ENVIRONMENT
##########

import numpy as np
import os
import json
import pandas as pd
import requests
from bs4 import BeautifulSoup
import sys
#sys.tracebacklimit = 0 # turn off the error tracebacks
import datetime
from datetime import date, timedelta
import dotenv
import boto3

import country_functions as cf

# set working directory to location of .env file 
env_path = '..' # change this location to run on your own directory
original_path = os.getcwd()
os.chdir(env_path)

# Load .env and save variables
dotenv.load_dotenv()
access_key_id=os.getenv('s3_guler_key') # change to name of access key id in your own .env
access_key_secret=os.getenv('s3_guler_id') # change to name of access key secret in your own .env

# change back working directory to notebook location
os.chdir(original_path)

# Connect to S3 and import metadata

s3 = boto3.client(
    service_name='s3',
    region_name='us-east-1',
    aws_access_key_id=access_key_id,
    aws_secret_access_key=access_key_secret
)

##########
# STEP 1 - CREATE LIST OF DATES TO PULL AND SET UP WEBSCRAPE
##########

today = date.today()
most_recent_chart_date = (today - datetime.timedelta(days=(today.weekday() + 2), weeks=0)) # charts always released on Saturday
first_date = date(1959, 1, 1) # country charts start in 1959

chart_dates = [most_recent_chart_date] # set up list to store dates, starting with latest

# loop through weeks until start date hit
current_date = most_recent_chart_date
while current_date >= first_date:
    current_date = current_date - timedelta(weeks=1)
    chart_dates.append(current_date)
    
    
##########
# STEP 2 - PULL AND SAVE CHARTS
##########

# set params for saving files (these will change)
aws_client = s3
bucket = 'country-bucket-guler'
temp_folder = '..\\data\\temp'
aws_folder = 'data/charts' # change if pop
url = 'https://www.billboard.com/charts/country-songs/' # change if pulling pop charts

# check to see which charts still need to be pulled (allows for charts to be pulled in multiple steps)
chart_files_raw = cf.list_files(s3, bucket, 'data/charts')
pulled_charts = [file.split('/')[-1].replace('CHARTS_', '').replace('.csv', '') for file in chart_files_raw if 'CHARTS_' in file]
charts_to_pull = [chart for chart in chart_dates if str(chart) not in pulled_charts]

# loop through unpulled dates and save charts to s3
for cd in charts_to_pull:
    charts = cf.pull_charts(url, cd)
    filename = f'CHARTS_{str(cd)}.csv'
    cf.csv_to_s3(charts, filename, aws_client, bucket, temp_folder, aws_folder)