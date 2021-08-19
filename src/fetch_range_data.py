# This function is used to fetch data from the coinbase api given a specified range of time, using YYYY-MM-DD format


# imports
import pandas as pd
import requests
import json
import datetime as dt
from datetime import timedelta
import os
from os import mkdir
from os.path import exists
import warnings

from fetch_daily_data import fetch_daily_data

today = dt.date.today()
yesterday = today - timedelta(days=1)

final = dt.date.today() - timedelta(days=1)

def fetch_range_data(symbol, start, end, always_fetch=False):
    '''
    Gets data from api between specified dates and saves .csv at given path

    Params:
    symbol (string) - symbol pair desired ex. BTC/USD
    start (string) - initial desired date in YYYY-MM-DD format
    end (string) - final desired date in YYYY-MM-DD format
    *kwargs


    Returns:
    df (pandas dataframe) -  dataframe of all data
    '''

    # check type of input, convert to dt.date object if needed
    if type(start) != dt.date:

        if type(start) == str:

            #use method to convert from string to dt.date type
            start = dt.datetime.strptime(start, '%Y/%m/%d')

        try:
            start = start.date()

        except:
            raise TypeError("Invalid type: provide string date or datetime object")

    if type(end) != dt.date:

        # check type of input, convert to dt.date object
        if type(end) == str:

            #use method to convert from string to dt.date type
            end = dt.datetime.strptime(end, '%Y/%m/%d')

        try:
            end = end.date()

        except:
            raise TypeError("Invalid type: provide string date or datetime object")

    # define  create folder name that does not change with repeated queries
    # final = end
    combined_file_path = f'../data/coinbase/daily_combined/combined_coinbase_{end}.csv'

    #if option to always query data, ignore
    # otherwise, return data from file and ignore code
    if not always_fetch and exists(combined_file_path):

        df = pd.read_csv(combined_file_path)
        print('File exists, returned as df')
        return df

    # create path for folder name to contain raw data
    path = f'../data/coinbase/daily_raw/{end}'

    # make folder for data or make exception
    try:
        mkdir(path)

    except:
        warnings.warn(f"{path} already exists")

   # get all available data between start and end dates

    # instantiate indexer "i_date"
    # if today, subtract 1 day to get only complete data else, set indexer to end
    i_date = yesterday if end == today else end

    # # create placeholder for date range
    # place_date = i_date

    #initial date searching for data on coinbase api
#     first_record = dt.date(2015,7,20)
    first_record = start

    # define "final" to store end date provided
    final = end

    # wrap in while loop to prevent run away queries
    while i_date > first_record:
        #define start and end dates for query
        end = str(i_date)
        # coinbase limits requests to 300 entries, limit request to 300 days each
        i_date -= timedelta(days=300)
        
        a = first_record
        b = i_date+timedelta(days=1)
        
        # respect first date constraint\
        start = max(a, b) # todo- whay am I adding 1?

        # fetch and save .csvs for each query
        fetch_daily_data(symbol, start, end, final = final)


    # combine the data
    cb_list = []
    # update path
    path = f'../data/coinbase/daily_raw/{final}'

    for root, dirs, files in os.walk(path):
        if root == path:
            for name in files:
                cb_list.append(os.path.join(root,name))


    # add each response file to single DataFrame
    df = pd.DataFrame()
    for csv in cb_list:
        df = df.append(pd.read_csv(csv), ignore_index=True)

    # display warnings if duplicates detected

    if not df.drop_duplicates().equals(df):
      warnings.warn('Duplicate entries detected.')


    # change index to datetime objects for use in timeseries modeling
    df.set_index(pd.to_datetime(df['date']), drop=True, inplace=True)
    df.pop('date')

    # save to combined filepath, defined at beginning of function
    df.to_csv(combined_file_path)

    # return final df
    return df
