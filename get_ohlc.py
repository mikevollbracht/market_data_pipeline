# Resource https://pypi.org/project/yfinance/

from pandas_datareader import data as pdr
import pandas as pd
import yfinance as yf
import os
import datetime
import time

# override for data reader
yf.pdr_override()


def get_ohlc(ticker_list='stocks', start_date='2015-01-01', ohlc_directory='ohlc_data_stocks'):
    # set dates
    run_timestamp = datetime.datetime.now()
    end_date = datetime.date.today()

    # get list of tickers, else take a single ticker as an parameter
    if ticker_list == 'stocks':
        tickers = pd.read_csv('tickers/stock_tickers.csv')
        tickers = tickers['Ticker']
    elif ticker_list == 'other':
        tickers = pd.read_csv('tickers/other.csv')
        tickers = tickers['Ticker']
    elif ticker_list == 'crypto':
        tickers = pd.read_csv('tickers/crypto_tickers.csv')
        tickers = tickers['Pair']
    else:
        tickers = [ticker_list]

    # initial settings to prevent yahoo throttling
    batch_max = 10
    call_count = 0

    if not os.path.isdir(f'{ohlc_directory}'):
        os.makedirs(f'{ohlc_directory}')

    if not os.path.isdir('logs'):
        os.makedirs('logs')

    for count, ticker in enumerate(tickers[:20]):
        ohlc_data = pdr.get_data_yahoo(
            ticker, start=start_date, end=end_date)
        call_count += 1

        if ohlc_data.shape[0] > 100:
            # clean missing rows
            ohlc_data.fillna(method='ffill', inplace=True)
            ohlc_data.fillna(method='bfill', inplace=True)
            # clean volume
            ohlc_data['Volume'].replace(
                to_replace=0, method='ffill', inplace=True)
            ohlc_data['Volume'].replace(
                to_replace=0, method='bfill', inplace=True)

            # pandas will overwrite the existing file
            ohlc_data.to_csv(f'{ohlc_directory}/{ticker}.csv')
        else:
            with open('logs/ohlc_error_log.txt', 'a') as error_log:
                error_log.write(f'{run_timestamp}: {ticker} not found\n')

        # To prevent throttling by yahoo
        if call_count % batch_max == 0:
            call_count = 0
            print(
                f'Tickers Processed: {count+1}, waiting to prevent throttling.')
            time.sleep(2)


# call stocks
# get_ohlc()

# call individual ticker
# get_ohlc('TSLA')

# call crypto
# get_ohlc('crypto', ohlc_directory='ohlc_data_crypto')

# call index
get_ohlc('other', ohlc_directory='ohlc_data_other')