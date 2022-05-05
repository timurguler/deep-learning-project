'''
PROCEDURE 3 - PULLING AND SAVING LYRICS

TIMUR GULER
05.04.2022

WARNING - THIS PROCESS IS LIKELY TO TAKE MULTIPLE DAYS
'''

##########
# STEP 0 - IMPORT NECESSARY PACKAGES AND SET UP AWS ENVIRONMENT, GENIUS DRIVER
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
import lyricsgenius
import regex as re
import pickle
import country_functions as cf

# set working directory to location of .env file
env_path = '..'
original_path = os.getcwd()
os.chdir(env_path)

# Load .env and save variables
dotenv.load_dotenv()
genius_client_id=os.getenv('genius_client_id')
genius_secret=os.getenv('genius_secret')
genius_access_token=os.getenv('genius_access_token')
genius = lyricsgenius.Genius(genius_access_token)
genius.verbose = False # Turn off status messages
access_key_id=os.getenv('s3_guler_key')
access_key_secret=os.getenv('s3_guler_id')

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
# STEP 1 - DETERMINE SONGS TO ADD
##########

LIB = pd.read_csv(s3.get_object(Bucket=bucket, Key='data/LIB.csv')['Body']).set_index('song_id') # song meta table, change key for pop
songs_to_add = list(LIB.index)

all_lyrics = pd.read_csv(s3.get_object(Bucket=bucket, Key='data/LYRICS.csv')['Body']).set_index('song_id') # lyrics pulled so far (ignore if process not yet started) - change key for pop
added_songs = list(all_lyrics.index)

##########
# STEP 2 - ADD SONGS
##########

while set(added_songs) != set(songs_to_add): # use while loop due to timeout errors
    try:
        # get current state of lyrics to see which songs have already been pulled
        all_lyrics = pd.read_csv(s3.get_object(Bucket=bucket, Key='data/LYRICS.csv')['Body']).set_index('song_id') # change key if pop
        added_songs = list(all_lyrics.index)
        
        # add remaining songs and save to s3 bucket
        for song in set(songs_to_add).difference(added_songs):
            title = LIB.loc[song].title
            artist = LIB.loc[song].artist_strip

            lyrics = cf.get_lyrics(title, artist, genius)

            song_df = pd.DataFrame({'lyrics' : lyrics}, index=[song])
            song_df.index.name = 'song_id'

            all_lyrics = pd.concat((all_lyrics, song_df), axis=0)
            cf.csv_to_s3(all_lyrics, 'POP-LYRICS.csv', s3, bucket, temp_folder, aws_folder)
            
    except:
        pass