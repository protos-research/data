#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Apr 25 23:05:09 2018

@author: SebastianArr
"""

# run time for full download: ~5 minutes
# run time for daily update: ~1 minute


import datetime
import getpass
import mysql.connector as mdb
import pandas as pd
import requests
import time
from auxiliary_functions import insert_into_table


# Connect to the MySQL instance
db_host = 'localhost'
db_user = 'root'
db_name = 'securities_master'
db_pass = getpass.getpass('Enter database password: ')
con = mdb.connect(host=db_host, user=db_user, passwd=db_pass, db=db_name)





# get all required cryptocurrency combinations from table 'cryptocurrencies_combinations' and directly convert keys to original names
def get_cryptocurrencies_combinations():

    sql = """SELECT all_comb.id as 'id', vendors.name as 'vendors',  exchanges.name as 'exchanges', symbols.ticker as 'symbols', symbols_ref.ticker as 'symbols_ref'
          FROM cryptocurrencies_combinations as all_comb
          left join cryptocurrencies_data_vendors as vendors on all_comb.cryptocurrencies_data_vendors_id=vendors.id
          left join cryptocurrencies_exchanges as exchanges on all_comb.cryptocurrencies_exchanges_id=exchanges.id
          left join cryptocurrencies_symbols as symbols on all_comb.cryptocurrencies_symbols_id=symbols.id
          left join cryptocurrencies_symbols_reference_currencies as symbols_ref on all_comb.cryptocurrencies_symbols_reference_currencies_id=symbols_ref.id
          order by all_comb.id;"""

    all_combinations = pd.read_sql_query(sql,con=con)
    
    return all_combinations



# download prices from cryptocompare
def download_quotes(symbol_info):
    prices = []
    columns = ['cryptocurrencies_combinations_id','price_date','created_date','open_price','high_price','low_price','close_price','volume_from','volume_to']
    table = 'cryptocurrencies_daily_prices'
    
    id = symbol_info['id']
    symbol = symbol_info['symbols']
    symbol_ref = symbol_info['symbols_ref']
    exchange = symbol_info['exchanges']


    # get latest date available in database
    sql = "SELECT max(price_date) as date_max from cryptocurrencies_daily_prices where cryptocurrencies_combinations_id=%s" % (id)
    date_max = pd.read_sql_query(sql, con=con)['date_max'][0]
    
    
    # prepare url string
    # in particular check if we need to load all available data or just the latest days
    url = "https://min-api.cryptocompare.com/data/histoday?fsym=%s&tsym=%s&e=%s" % (symbol, symbol_ref, exchange)
    if date_max is None:   # no data for this ticker available yet --> get all available data
        url = url + '&allData=true'
    else:
        delta = now - date_max   # time difference between now and latest day that we have prices for
        delta = str(delta.days)
        url = url + '&limit=' + delta      # if limit is set e.g. to 1 we actually download 2 data points (including [0])
                                           # so in fact we always download one more day which is also safer because of possible rounding issues
                                           # one special case if limit is 0 --> we still download last 2 data points as if limit is 1
    
    
    # if we download a lot of times (which we do in the loop over all tickers) it fails occasionally
    # if this "never" succeeds the while loop runs forever
    # in practice it always worked after trying a few times so the current implementation works quite well (and quick!)
    while True:
        response = requests.get(url)
        if response.json()['Response']=='Success':   # download successful --> take data and leave loop
            data = response.json()['Data']
            break
        else:    # download not successful --> wait a second and try again
            time.sleep(1)
            
            
    for d in data:
        date_data = datetime.datetime.utcfromtimestamp(d['time'])
        if date_max is None:      # add all prices
            prices.append( (id, date_data, now, d['open'], d['high'], d['low'], d['close'], d['volumefrom'], d['volumeto']) )
        else:  
            if date_data>date_max:    # only add prices if it is really a more recent date than already available in the database
                                      # this will usually cut off the extra day mentioned above 
                prices.append( (id, date_data, now, d['open'], d['high'], d['low'], d['close'], d['volumefrom'], d['volumeto']) )
     
    return prices, columns, table



if __name__ == "__main__":
    
    # set up timer    
    start = time.time()

    # get current date to add timestamp to every entry in the database
    now = datetime.datetime.utcnow()
    
    all_combinations = get_cryptocurrencies_combinations()
    n = all_combinations.shape[0]
    
    all_combinations['num_added_entries'] = 0
    
    
    # get prices and add directly to database
    for i, row in all_combinations.iterrows():
        print ("----------------------------------------------------------")
        print( "Iteration %s/%s: %s/%s on %s from %s" % (i+1, n, row['symbols'], row['symbols_ref'], row['exchanges'], row['vendors']))
        
        prices = download_quotes(row)
        insert_into_table(prices,con)
        
        all_combinations.loc[i,'num_added_entries'] = len(prices[0])
        
    print ("----------------------------------------------------------")
    print('\n')
        
    
    # prepare file with number of added entries
    file_name = '../log_info/cryptocurrencies_added_summary_' + now.strftime('%Y_%m_%d') + '.csv'
    all_combinations.to_csv(file_name, index=False, sep=';')
    
    print ("----------------------------------------------------------")
    print ('Check summary of added cryptocurrency prices:')
    print (file_name)
    print ("----------------------------------------------------------")


    done = time.time()
    elapsed = (done - start)/60.    # elapsed time in minutes
    elapsed = round(elapsed)
    
    print ('Time needed in minutes: ' + str(elapsed))
    print ("----------------------------------------------------------")

con.close()




