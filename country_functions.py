##########
# Functions to assist with country lyrics project
# Author - Timur Guler (https://github.com/timurguler)
##########

##########
# PREEQUISISITES
##########

import pandas as pd
import numpy as np
import datetime
from datetime import date, timedelta
from bs4 import BeautifulSoup
import requests
import json
import boto3
import os
import regex as re
import nltk
import gensim.downloader

##########
# SECTION I - AWS CONNECTIONS
##########

def pull_charts(url : str, date : datetime.date):
    """
    GOAL - pull hot country chart for a given (valid date)
    INPUTS - 
        url - url to access (hot country charts base url)
        date - chart date
    OUTPUTS - 
        dataframe of charts, with song title, artist, date, rank
    """
    r = requests.get(url+str(date), headers = {'user-agent': 'txx3ej@virginia.edu'})
    wnrn = BeautifulSoup(r.text, 'html')
    
    list_songs = wnrn.body.find("div",{"id":"main-wrapper"}).main \
    .find_all("div",{"class":"o-chart-results-list-row-container"})
    
    return pd.DataFrame(([w.h3.text.strip(), 
                        w.span.find_next('span').text.strip(),
                        i+1,
                        date] for i,w in enumerate(list_songs)),
                        columns=['song', 'artist', 'rank','date'])

def csv_to_s3(csv, filename, aws_client, bucket, temp_folder, aws_folder):
    '''
    GOAL - move pd dataframe or other csv-compatible file to aws s3, utilizing save and delete in local temp folder
    INPUTS - 
        csv - python object to upload as csv
        filename - desired end filename (str)
        aws_client - relevant aws client (aws client object)
        bucket - s3 bucket name (s3)
        temp_folder - path of temp folder used for local save
        aws_folder - name of AWS folder where file is to be saved
    OUTPUTS - None
    '''
    temp_file = os.path.join(temp_folder, filename)
    aws_file = aws_folder + '/' + filename
    csv.to_csv(temp_file)
    aws_client.upload_file(temp_file, bucket, aws_file)
    os.remove(temp_file)
    
def list_files(aws_client, bucket, prefix):
    '''
    GOAL - list all files in a certain S3 folder
    INPUTS - 
        aws_client - AWS client associated with folder (AWS client object)
        bucket - name of bucket (str)
        prefix - folder name (str)
        
    OUTPUTS - 
        files - list of files in bucket
    '''
    paginator = aws_client.get_paginator('list_objects_v2')
    pages = paginator.paginate(Bucket=bucket, Prefix=prefix)

    files_dict = {}

    page_num = 1
    for page in pages:
        files_dict[page_num] = [file.get('Key') for file in page.get('Contents')]
        page_num += 1
        
    files = []

    _ = [[files.append(val) for val in v] for k, v in files_dict.items()]
    
    return files

##########
# SECTION II - GENIUS CONNECTIONS
##########

def get_lyrics(title : str, artist : str, genius_driver):
    '''
    GOAL - pull lyrics string for one song
    INPUTS - 
        title - song title
        artist - artist name
        genius_driver - instantiated genius driver object created with api token
    OUTPUTS - 
        lyrics - a string containing the song's lyrics
    '''
    
    raw_lyrics = genius_driver.search_song(title=title, artist=artist)
    if raw_lyrics:
        lyrics = re.sub(r'[0-9]+Embed', '', re.sub(r'\[[^]]*\]', '', raw_lyrics.lyrics))
        lyrics = '\n'.join(lyrics.split('\n')[1:])
    else:
        lyrics = 'song not found'
    return lyrics

##########
# SECTION III - PROCESSING CORPUS (MAIN)
##########

def collapse_and_save(aws_client, aws_bucket, input_file, output_folder, temp_folder, output_name, OHCO, level, splitter, col_name_initial, col_name_new):
    
    '''
    GOAL - collapse OHCO by one level while pruning for invalid responses (mispulled novels, TV episodes, etc) based on section length
    INPUTS - 
        aws_client - AWS client associated with folder (AWS client object)
        aws_bucket - name of S3 bucket (str)
        input_file - name of original (pre-collapse) table in S3 (including folder)
        output_folder - destination folder for collapsed output (S3)
        temp_folder - path of temp folder used for local save
        output_name - filename for collpased table
        OHCO - list of OHCO names
        level - (int), OHCO level of collapsed table
        splitter - str used for splitting (e.g. '\n\n')
        limit (deprecated) - maximum length of post-split section (e.g. if you dont want sections with more than 100 lines, this would be 100)
        col_name_initial - content column name (separate from OHCO, e.g. 'section_lyrics') for pre-collapse table
        col_name_new - content column name (separate from OHCO, e.g. 'line_lyrics') for post-collapse table
    OUTPUTS - 
        no returned outputs - processes table and saves to S3
    '''
    
    # pull in original and set index, ensure target column is string type
    original = pd.read_csv(aws_client.get_object(Bucket=aws_bucket, Key=input_file)['Body'])
    original = original.set_index(OHCO[:level-1])
    original[col_name_initial] = original[col_name_initial].astype(str)
    
    # reduce original table to only valid values (e.g. less than 20 sections per song, 50 lines per section, 100 words per line)
    #reduced = original[original[col_name_initial].apply(lambda x : len(x.split(splitter))) < limit]
    
    # split, reset index, save
    new = original[col_name_initial].str.split(splitter, expand=True).stack().to_frame(col_name_new)
    new[col_name_new] = new[col_name_new].str.strip() # remove whitespace
    new.index.names = OHCO[:level]
    csv_to_s3(new, output_name, aws_client, aws_bucket, temp_folder, output_folder)
    
def prep_for_analysis(song : str, punctuation : str):
    '''
    GOAL - prep song lyrics for analysis by converting to lowercase, removing punctuation, and replacing symbols and spaces with end tokens
           meant to be applied to column in dataframe
    INPUTS - 
        song - lyrics to song
        punctuation - string containing list of all punctuation to remove (can be imported from string package)
    OUTPUTS - 
        prepped - a processed version of the string
    '''    
    prepped = song.translate({ord(punc): '' for punc in punctuation}) # remove punc
    prepped = ' '.join(prepped.lower().replace('\n\n', ' <s> ').replace('\n', ' <l> ').replace('embed', ' <e>').split()) # convert to lower and replace end tokens
    return prepped

def tokenize_tag(line_level : pd.DataFrame, OHCO, col_name):
    '''
    GOAL - tokenize lines using nltk's POS tagger and create collapsed token table
    INPUTS - 
        line_level - dataframe of corpus at the line level (equivalent of sentence, typical level for pos tagging)
        OHCO - list of OHCO names
        col_name - name of column in line_level where content is stored (e.g. "line_lyrics")
    OUTPUTS - 
        tokens - a table of tokens and their POS at the full OHCO level
    '''    
    line_level = line_level.set_index(OHCO[:-1])

    tokens = line_level[col_name].apply(lambda x: pd.Series(nltk.pos_tag(nltk.word_tokenize(x))))\
                .stack()\
                .to_frame('pos_tuple')

    tokens['token'] = tokens.pos_tuple.apply(lambda x : x[0])
    tokens['pos'] = tokens.pos_tuple.apply(lambda x : x[1])
    tokens.index.names = OHCO
    
    return tokens[['token', 'pos']]

def is_clean(string, pattern):
    '''
    GOAL - determine whether a string meets a particular pattern (necessary to functionize rather than lambda due to if/else)
    INPUTS - 
        string - the string to be searched for the pattern
        pattern - regex pattern for comparison
        col_name - name of column in line_level where content is stored (e.g. "line_lyrics")
    OUTPUTS - 
        False if pattern identified, true otherwise
    '''    
    if pattern.search(string):
        return False
    else:
        return True
    
##########
# SECTION IV - PROCESSING CORPUS (RNN)
##########
    
def reduce(word, vocab):
    '''
    GOAL - determine whether a token is in or out of vocab (used for RNN vocab reduction)
    INPUTS - 
        word - string
        vocab - list of strings (words that make up the vocab)
    OUTPUTS - 
        same word if in vocab, word 'replace'
    '''  
    if word in vocab:
        return word
    else:
        return 'replace'
    
def replace_with_similar(word, embeddings, vocab):
    '''
    GOAL - replace an out-of-vocab word with the closest in-vocab word in the glove word embedding space, or with 'no match' if no in-vocab words in top 100
    INPUTS - 
        word - string
        embeddings - gensim embeddings object
        vocab - list of strings (words that make up the vocab)
    OUTPUTS - 
        either closest in-vocab word or 'no match' (if no close in-vocab matches)
    ''' 
    if word in vocab:
        return word
    else:
        try:
            return [x[0] for x in embeddings.most_similar(word, topn=100) if x[0] in vocab][0]
        except:
            return 'no match'