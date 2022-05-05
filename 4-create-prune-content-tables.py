'''
PROCEDURE 4 - CREATING CORPUS USING ORDERED HEIRARCHY OF CONTENT OBJECTS

TIMUR GULER
05.04.2022
'''

##########
# STEP 0 - IMPORT NECESSARY PACKAGES AND SET UP AWS ENVIRONMENT
##########

import numpy as np
from collections import Counter
import os
import json
import pandas as pd
import requests
from bs4 import BeautifulSoup
import sys
import datetime
from datetime import date, timedelta
import dotenv
import boto3
import country_functions as cf
import nltk

# set working directory to location of .env file
env_path = '..'
original_path = os.getcwd()
os.chdir(env_path)

# Load .env and save variables

dotenv.load_dotenv()
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
# STEP 1 - MERGE LIB AND LYRICS TO GET CORPUS WITH OHCO
##########

# pull in CORPUS and LIB

CORPUS = pd.read_csv(s3.get_object(Bucket=bucket, Key='data/LYRICS.csv')['Body'])
CORPUS = CORPUS.query("lyrics!='song not found'") # remove songs where lyrics not found
CORPUS.lyrics = CORPUS.lyrics.astype(str)
CORPUS = CORPUS.set_index('song_id')

LIB = pd.read_csv(s3.get_object(Bucket=bucket, Key='data/LIB.csv')['Body'])
LIB = LIB.set_index('song_id')

# merge, set OHCO, save to S3

CORPUS = CORPUS.merge(LIB[['decade', 'year', 'artist_strip', 'gender', 'title']], left_index=True, right_index=True)

OHCO = ['decade', 'year', 'gender', 'artist_strip', 'title', 'section', 'line', 'token']
CORPUS = CORPUS.set_index(OHCO[:5])

cf.csv_to_s3(CORPUS, 'CORPUS.csv', s3, bucket, temp_folder, aws_folder)

##########
# STEP 2 - REDUCE CORPUS BASED ON SONG LENGTH
##########

OHCO = ['decade', 'year', 'gender', 'artist_strip', 'title', 'section', 'line', 'token']
CORPUS = pd.read_csv(s3.get_object(Bucket=bucket, Key='data/CORPUS.csv')['Body'])
CORPUS.lyrics = CORPUS.lyrics.astype(str)
CORPUS = CORPUS.set_index(OHCO[:5])

# pre-process by removing punctuation, converting to lower, creating tokens for end line, section, song
CORPUS['prepped'] = CORPUS.lyrics.map(lambda x: cf.prep_for_analysis(x, punctuation))

# only use songs less than 1000 words (the rest are likely mispulls)
CORPUS_REDUCED = CORPUS[CORPUS.lyrics.apply(lambda x : len(x.split(' '))) < 1000]

# save to S3
cf.csv_to_s3(CORPUS_REDUCED, 'CORPUS-REDUCED.csv', s3, bucket, temp_folder, aws_folder)

##########
# STEP 3 - COLLAPSE OHCO AND SAVE TO S3
##########

corpus2line_params = {
    'aws_client' : s3,
    'input_file' : 'data/CORPUS-REDUCED.csv',
    'output_folder' : 'data',
    'temp_folder' : '..\\data\\temp',
    'aws_bucket' : 'country-bucket-guler',
    'output_name' : 'SECTION-REDUCED.csv',
    'OHCO' : ['decade', 'year', 'gender', 'artist_strip', 'title', 'section', 'line', 'token'],
    'level' : 6,
    'splitter' : '<s>',
    'col_name_initial' : 'prepped',
    'col_name_new' : 'section_lyrics'
} 

sec2line_params = {
    'aws_client' : s3,
    'input_file' : 'data/SECTION-REDUCED.csv',
    'output_folder' : 'data',
    'temp_folder' : '..\\data\\temp',
    'aws_bucket' : 'country-bucket-guler',
    'output_name' : 'LINE-REDUCED.csv',
    'OHCO' : ['decade', 'year', 'gender', 'artist_strip', 'title', 'section', 'line', 'token'],
    'level' : 7,
    'splitter' : '<l>',
    'col_name_initial' : 'section_lyrics',
    'col_name_new' : 'line_lyrics'
} 

line2token_params = {
    'aws_client' : s3,
    'input_file' : 'data/LINE-REDUCED.csv',
    'output_folder' : 'data',
    'temp_folder' : '..\\data\\temp',
    'aws_bucket' : 'country-bucket-guler',
    'output_name' : 'TOKEN-REDCUED.csv',
    'OHCO' : ['decade', 'year', 'gender', 'artist_strip', 'title', 'section', 'line', 'token'],
    'level' : 8,
    'splitter' : ' ',
    'col_name_initial' : 'line_lyrics',
    'col_name_new' : 'TOKEN'
} 

cf.collapse_and_save(**corpus2line_params)
cf.collapse_and_save(**sec2line_params)
cf.collapse_and_save(**line2token_params)