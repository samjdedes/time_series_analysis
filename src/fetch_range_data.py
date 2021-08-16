# This function is used to fetch data from the coinbase api given a specified range of time, using YYYY-MM-DD format


# imports
import pandas as pd
import requests
import json
import datetime as dt
from datetime import timedelta
import os
from os import mkdir
import warnings

from fetch_daily_data import fetch_daily_data

today = dt.date.today()
yesterday = today - timedelta(days=1)

final = dt.date.today() - timedelta(days=1)

def fetch_range_data(symbol, start, end, path=None, combine=True):
    '''
    Gets data from api between specified dates and saves at given path

    Params:
    symbol (string) - symbol pair desired ex. BTC/USD
    start (string) - initial desired date in YYYY/MM/DD format
    end (string) - final desired date in YYYY/MM/DD format
    *kwargs

    Returns:
    .csv at default or specified path with requested data
    '''
   # get all available data between start and end dates
    # instantiate indexer "i_date"
    i_date = dt.date.today() - timedelta(days=1)

    # create placeholder for date range
    place_date = i_date

    #initial date searching for data on coinbase api
    first_record = dt.date(2015,7,20)

    # wrap in while loop to prevent run away queries
    while i_date > first_record:

        # coinbase limits requests to 300 entries, limit request to 300 days each
        i_date = place_date - timedelta(days=300)

        #define start and end dates for query
        end = str(place_date)
        start = str(i_date+timedelta(days=1))

        fetch_daily_data(symbol, start, end)

        # set last date to date to continue loop
        place_date = i_date


    if combine:
        cb_list = []
        if not path:
            path = f'../data/coinbase/daily_raw/{end}'

        for root, dirs, files in os.walk(path):
            if root == path:
                for name in files:
                    cb_list.append(os.path.join(root,name))


        # add each response file to single DataFrame
        df = pd.DataFrame()
        for my_df in cb_list:
            df = df.append(pd.read_csv(my_df), ignore_index=True)


        # display warnings if duplicates detected

        if not df.drop_duplicates().equals(df):
          warnings.warn('Duplicate entries detected.')


        # change index to datetime objects for use in timeseries modeling
        df.set_index(pd.to_datetime(df['date']), drop=True, inplace=True)
        df.pop('date')

        #sort and save
        df.sort_index()

        return df.to_csv(f'../data/combined_coinbase_{end}.csv')
