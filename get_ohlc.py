#Resource https://pypi.org/project/yfinance/

from pandas_datareader import data as pdr
import yfinance as yf
import os 
import datetime
import time
import pandas as pd

#override for data reader
yf.pdr_override()

#time of run for error log 
run_timestamp = datetime.datetime.now()
start_date = '2016-01-01'
end_date = datetime.date.today()

#pause after 50 to prevent throttling 
batch_max = 10
call_count = 0

#get tickers
tickers = pd.read_csv('tickers/my_tickers.csv')
tickers = tickers['Ticker']

def get_ohlc():
    if not os.path.isdir('ohlc_data'):
        os.makedirs('ohlc_data')

    for count, ticker in enumerate(tickers): 
        if not os.path.exists(f'ohlc_data/{ticker}.csv'):
            # ohlc_data = pdr.get_data_yahoo(ticker, start=start_date ,end=end_date)
            # call_count += 1
        
            # if ohlc_data.shape[0] > 0: 
            #     ohlc_data.to_csv(f'ohlc_data/{ticker}.csv')
            # else: 
            #     with open('ohlc_data/error_log.txt', 'a') as error_log:
            #         error_log.write(f'{run_timestamp}: {ticker} not found\n')
                    
            # #To prevent throttling by yahoo
            # if call_count % batch_max == 0: 
            #     call_count = 0
            #     print(f'Ticker Count: {count}, waiting to prevent throttling.')
            #     time.sleep(2)
            pass
                    
        else: 
            print(f'Already have {ticker}')
            #open existing csv 
            df = pd.read_csv(f'ohlc_data/{ticker}.csv', index_col=0)
            # Get last date in the df and add 1 date 
            last_date = datetime.datetime.strptime(df.index.values[-1],'%Y-%m-%d').date() + datetime.timedelta(days=1)
            print(last_date)
            

# get_ohlc()