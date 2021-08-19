# function queries coinbase api between given dates

import pandas as pd
import requests
import json
from os import mkdir
from sys import stderr


def fetch_daily_data(symbol, start, end, final):
    # define path
    path = f'../data/coinbase/daily_raw/{final}'

    # symbol must be in format XXX/XXX ie. BTC/EUR
    coin, fiat = symbol.split('/')
    symbol = coin + '-' + fiat
    url = f'https://api.pro.coinbase.com/products/{symbol}/candles?granularity=86400&start={start}&end={end}'
    response = requests.get(url)
    if response.status_code == 200:  # check to make sure the response from server is good
        data = pd.DataFrame(json.loads(response.text), columns=['unix', 'low', 'high', 'open', 'close', 'volume'])
        data['date'] = pd.to_datetime(data['unix'], unit='s')  # convert to a readable date
        data['vol_fiat'] = data['volume'] * data['close']      # multiply the BTC volume by closing price to approximate fiat volume

        # if we failed to get any data, print an error...otherwise write the file
        if data is None:
            print(f"Did not return any data from Coinbase for this symbol - {symbol}", file=stderr)
        else:
            data.to_csv(f'{path}/Coinbase_{coin + fiat}_data_{start}_{end}.csv', index=False)


    else:
        print("Did not receieve OK response from Coinbase API")
