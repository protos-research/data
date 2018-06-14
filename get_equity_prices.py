#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Apr 25 23:05:09 2018

@author: SebastianArr
"""

# run time for full download: ~17 minutes
# run time for daily update: ~9 minutes


import datetime
import getpass
import mysql.connector as mdb
import pandas as pd
import requests
import time
import re
from auxiliary_functions import insert_into_table


# Connect to the MySQL instance
db_host = 'localhost'
db_user = 'root'
db_name = 'securities_master'
db_pass = getpass.getpass('Enter database password: ')
con = mdb.connect(host=db_host, user=db_user, passwd=db_pass, db=db_name)





# get all required cryptocurrency combinations from table 'cryptocurrencies_combinations' and directly convert keys to original names
def get_equities_combinations():

    sql = """SELECT all_comb.id as 'id', vendors.name as 'vendors',  exchanges.name as 'exchanges', symbols.ticker as 'symbols'
          FROM equities_combinations as all_comb
          left join equities_data_vendors as vendors on all_comb.equities_data_vendors_id=vendors.id
          left join equities_exchanges as exchanges on all_comb.equities_exchanges_id=exchanges.id
          left join equities_symbols as symbols on all_comb.equities_symbols_id=symbols.id
          order by all_comb.id;"""

    all_combinations = pd.read_sql_query(sql,con=con)
    
    return all_combinations



# download prices from yahoo
def download_quotes(symbol_info):
    prices = []
    columns = ['equities_combinations_id','price_date','created_date','open_price','high_price','low_price','close_price','close_price_adjusted','volume']
    table = 'equities_daily_prices'
    
    id = symbol_info['id']
    symbol = symbol_info['symbols']

    
    ###################################################
    # extract cookie and crumb from yahoo page
    url = "https://finance.yahoo.com/quote/%s/?p=%s" % (symbol, symbol)
    
    while True:
        possible_download = True

        # get cookie
        try:
            response = requests.get(url)
            cookie = {'B': response.cookies['B']}
        except:
            possible_download = False
    
        
        # get crumb (only try if cookie is found)
        if possible_download:
            try:
                lines = response.content.decode('unicode-escape').strip().replace('}', '\n')
                lines = lines.split('\n')
                
                for l in range(0,len(lines)):
                    if re.findall(r'CrumbStore', lines[l]):
                        crumb = lines[l].split(':')[2].strip('"') 
                        break
                    if l==(len(lines)-1):   # loop ran to the end without finding crumb
                        possible_download = False
            except:
                possible_download = False
                
        if possible_download:
            break
        else:    # not successful --> wait a second and try again
            time.sleep(1)
    
    
    ###################################################
    # get latest date available in database
    sql = "SELECT max(price_date) as date_max from equities_daily_prices where equities_combinations_id=%s" % (id)
    date_max = pd.read_sql_query(sql, con=con)['date_max'][0]
    
    if date_max is None:   # no data for this ticker available yet --> start at 0 (1970)
        start_date = 0
    else:   # otherwise load only new data that has not been added to the table yet / fix issue with double loading of first days below
        start_date = int( date_max.replace(tzinfo=datetime.timezone.utc).timestamp() )    # need to explicitely provide utc "timezone"!!!
        
    end_date = int( now.replace(tzinfo=datetime.timezone.utc).timestamp() )


    ###################################################
    # call function to get actual prices
    url = "https://query1.finance.yahoo.com/v7/finance/download/%s?period1=%s&period2=%s&interval=1d&events=history&crumb=%s" % (symbol, start_date, end_date, crumb)

    while True:
        possible_download = True
        
        try: 
            data = requests.get(url, cookies=cookie)
            data = data.text.split("\n")[1:-1]
        except:
            possible_download = False
            
        if possible_download:
            break
        else:    # not successful --> wait a second and try again
            time.sleep(1)
       
        
    ###################################################
    for d in data:
        p = d.strip().split(',')
        
        date_data = datetime.datetime.strptime(p[0], '%Y-%m-%d')
        p  = p[1:]
        
        # convert strings to float
        # take care of missing data --> yahoo uses 'null' but None is needed when writing to the database to get null in mysql
        p = [None if x=='null' else float(x) for x in p]

        if date_max is None:   # add all prices
            prices.append( (id, date_data, now, p[0], p[1], p[2], p[3], p[4], p[5]) )
        else:
            if date_data>date_max:    # only add prices if it is really a more recent date than already available in the database
                prices.append( (id, date_data, now, p[0], p[1], p[2], p[3], p[4], p[5]) )

    return prices, columns, table



if __name__ == "__main__":
    
    # set up timer    
    start = time.time()

    # get current date to add timestamp to every entry in the database
    now = datetime.datetime.utcnow()    
 
    all_combinations = get_equities_combinations()
    n = all_combinations.shape[0]
    
    all_combinations['num_added_entries'] = 0
    
    
    # get prices and add directly to database
    for i, row in all_combinations.iterrows():
        print ("----------------------------------------------------------")
        print( "Iteration %s/%s: %s on %s from %s" % (i+1, n, row['symbols'], row['exchanges'], row['vendors']))
        
        prices = download_quotes(row)
        insert_into_table(prices,con)
        
        all_combinations.loc[i,'num_added_entries'] = len(prices[0])
        
    print ("----------------------------------------------------------")
    print('\n')
        
    
    # prepare file with number of added entries
    file_name = '../log_info/equities_added_summary_' + now.strftime('%Y_%m_%d') + '.csv'
    all_combinations.to_csv(file_name, index=False, sep=';')
    
    print ("----------------------------------------------------------")
    print ('Check summary of added equity prices:')
    print (file_name)
    print ("----------------------------------------------------------")


    done = time.time()
    elapsed = (done - start)/60.    # elapsed time in minutes
    elapsed = round(elapsed)
    
    print ('Time needed in minutes: ' + str(elapsed))
    print ("----------------------------------------------------------")

con.close()




