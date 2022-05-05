'''
PROCEDURE 2 - COMPILING SONG METADATA FOR UNIQUE SONGS (BEFORE PULLING LYRICS)

TIMUR GULER
04.27.2022
'''

##########
# STEP 0 - IMPORT NECESSARY PACKAGES AND SET UP AWS ENVIRONMENT
##########

import billboard

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

bucket='country-bucket-guler'
temp_folder = '..\\data\\temp'
aws_folder = 'data'

##########
# STEP 1 - AGGREGATE CHARTS AND SAVE TO AWS
##########

# list of chart files
pulled_charts = [file for file in cf.list_files(s3, bucket, 'data/charts') if 'CHARTS' in file] # change if pop

# create df of all charts
charts = pd.DataFrame()

for chart in pulled_charts:
    that_week = pd.read_csv(s3.get_object(Bucket=bucket, Key=chart)['Body'])
    charts = pd.concat((charts, that_week), axis=0)
    
charts = charts.iloc[:, 1:] # remove index column
charts.rename(columns={'song' : 'title'}, inplace=True) # change column name

# clean up formatting to enable aggregation by artist and successful lyric pulling
charts.title = charts.title.str.lower()
charts['artist_strip'] = charts.artist.apply(lambda x : x.lower().split(' x ')[0].split('featuring')[0].split( ' duet ')[0].split(' with ')[0].strip())

# save back to AWS
cf.csv_to_s3(charts, 'CHARTS.csv', s3, bucket, temp_folder, aws_folder) # change if pop

##########
# STEP 2 - AGGREGATE UNIQUE SONGS
##########

unique_songs = charts[['title', 'artist_strip']].drop_duplicates()
unique_songs.artist_strip = unique_songs.artist_strip.str.replace('\n', '')
unique_songs = unique_songs.query("artist_strip!='new' & artist_strip!='re-entry'").reset_index(drop=True) # artist sometimes listed as "new" or "re-entry"
unique_songs['song_id'] = unique_songs.title + '-' + unique_songs.artist_strip
LIB = unique_songs.set_index(['song_id']) # create song_id

##########
# STEP 3 - GET SONG METADATA
##########

# MIN/MAX DATES
min_max_dates = charts.groupby(['title', 'artist_strip']).date.agg(['min', 'max']).reset_index()
min_max_dates = min_max_dates.query("artist_strip!='new' & artist_strip!='re-entry'")
LIB = LIB.merge(min_max_dates, on=['title', 'artist_strip'], how='left', validate='one_to_one')

# HIGHEST RANK
highest_rank = charts.groupby(['title', 'artist_strip'])['rank'].min().to_frame('min_rank').reset_index()
highest_rank = highest_rank.query("artist_strip!='new' & artist_strip!='re-entry'")
LIB = LIB.merge(highest_rank, on=['title', 'artist_strip'], how='left', validate='one_to_one')

# YEAR AND DECADE
LIB['year'] = LIB['min'].apply(lambda x : int(x[:4]))
LIB['decade'] = LIB['year'].apply(lambda x : int(np.floor(x/10)*10))

# GENDER FOR TOP ARTISTS (REQUIRES MANUAL TAGGING)

# group top artists by decade and save results
top_artist_decade = LIB.groupby(['decade', 'artist_strip']).title.count().to_frame('num_hits')\
        .sort_values(['decade','num_hits'], ascending=False).groupby(['decade']).head(200)
cf.csv_to_s3(top_artist_decade, 'top_artists_decade.csv', s3, bucket, temp_folder, aws_folder)

# re-upload tagged results and clean inconsistenciea
artist_genders = pd.read_csv(s3.get_object(Bucket=bucket, Key='data/gendered_country_band.csv')['Body'])
artist_genders.artist_strip = artist_genders.artist_strip.str.strip()

artist_genders[artist_genders.artist_strip == 'rascal flatts'] = 'm'
artist_genders[artist_genders.artist_strip == 'gloriana'] = 'f'
artist_genders[artist_genders.artist_strip == 'highway 101'] = 'f'
artist_genders[artist_genders.artist_strip == 'robin lee'] = 'f'
artist_genders[artist_genders.artist_strip == 'the kendalls'] = 'd'

# add back to LIB table and save to S3
LIB = LIB.merge(artist_genders[['artist_strip', 'gender']].drop_duplicates(), how='left', on='artist_strip', validate='many_to_one')
LIB['song_id'] = LIB.title + '-' + LIB.artist_strip
LIB = LIB.set_index(['song_id'])
cf.csv_to_s3(LIB, 'LIB.csv', s3, bucket, temp_folder, aws_folder) # change if pop

##########
# STEP 4 - GET SEQUENTIAL LIST OF #1 SONGS
##########

num_ones = charts.query('rank==1')
TOP_SONGS_ORDER = num_ones[num_ones.title != num_ones.shift(1).title].reset_index(drop=True)
cf.csv_to_s3(TOP_SONGS_ORDER, 'TOP_SONGS_ORDER.csv', s3, bucket, temp_folder, aws_folder) # CHANGE IF POP