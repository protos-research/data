#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Apr 25 23:05:09 2018

@author: SebastianArr
"""

import datetime
import getpass
import mysql.connector as mdb
import requests
import pandas as pd
from auxiliary_functions import insert_into_table


# Connect to the MySQL instance
db_host = 'localhost'
db_user = 'root'
db_name = 'securities_master'
db_pass = getpass.getpass('Enter database password: ')
con = mdb.connect(host=db_host, user=db_user, passwd=db_pass, db=db_name)



n_currencies = 20
n_exchanges = 10
ref_list = ['BTC','USD']





# get coin information from cryptocompare
def get_coin_info():
    url = 'https://min-api.cryptocompare.com/data/all/coinlist'
    response = requests.get(url)
    data = response.json()['Data']
    return data



# create list of vendors
# only use 'cryptocompare' (hardcoded)
def define_vendor():
    vendors = []
    columns = ['name','website_url','support_email','created_date']
    table = 'cryptocurrencies_data_vendors'
        
    # check which vendors are already in table if any
    sql = "SELECT name from %s;" % (table)
    vendor_list_in = set(pd.read_sql_query(sql,con=con)['name'])

    vendor_list = set(['cryptocompare'])    # hardcoded
    vendor_list = vendor_list - vendor_list_in   # add only those which are not yet in

    if len(vendor_list)>0:
        vendors.append(('cryptocompare','https://www.cryptocompare.com/','info@cryptocompare.com',now))
        
    return vendors, columns, table



# define list of reference currencies for cryptocurrencies
# by default this will contain BTC and USD
# thus, later on we download exchange rates for all cryptocurrencies against BTC and USD
def define_crypto_ref_list(symbol_list, coin_info):
    symbols = []
    columns = ['ticker','instrument','name','proof_type','total_coin_supply','created_date']
    table = 'cryptocurrencies_symbols_reference_currencies'

    # check which symbols are already in table if any
    sql = "SELECT ticker from %s;" % (table)
    symbol_list_in = set(pd.read_sql_query(sql,con=con)['ticker'])

    symbol_list = set(symbol_list)
    symbol_list = symbol_list - symbol_list_in   # only consider tickers which are not already there

    if len(symbol_list)>0:
        symbol_list_cryptocompare = set(coin_info.keys())
         
        cryptos = symbol_list & symbol_list_cryptocompare    # only those are found on cryptocompare
        real_currencies = symbol_list - symbol_list_cryptocompare    # rest must be real currencies
    
        if len(real_currencies)>0:
            print ("----------------------------------------------------------")
            print ("The following list of reference \'currencies\' could not be found on \'cryptocompare\':")
            print (real_currencies)
            print ("They will be considered as real currencies.")
            print ("----------------------------------------------------------")
    
    
        # prepare list of symbols to add to database
        for i, ticker in enumerate(symbol_list):   
            if ticker in cryptos:
                TotalCoinSupply = coin_info[ticker]['TotalCoinSupply']
                if TotalCoinSupply=='N/A':
                    TotalCoinSupply = None
                symbols.append( ( ticker, 'cryptocurrency', coin_info[ticker]['CoinName'], coin_info[ticker]['ProofType'], TotalCoinSupply, now ) )
            else:
                symbols.append( ( ticker, 'real currency', ticker, None, None, now ) )
            
    return symbols, columns, table



# get list of top cryptocurrencies by market capitalization and top exchanges on which they are traded on by trading volumne against the reference currencies defined in 'define_crypto_ref_list'
def get_top_cryptocurrencies_exchanges(n_currencies, n_exchanges, coin_info):
    symbols = []
    symbols_columns = ['ticker','instrument','name','proof_type','total_coin_supply','created_date']
    symbols_table = 'cryptocurrencies_symbols'
    
    exchanges = []
    exchanges_columns = ['abbreviation','name','created_date']
    exchanges_table = 'cryptocurrencies_exchanges'

    all_combinations = pd.DataFrame(columns=['currency','ref_currency','exchange'])

    # check which symbols are already in table if any
    sql = "SELECT ticker from %s;" % (symbols_table)
    symbol_list_in = set(pd.read_sql_query(sql,con=con)['ticker'])
    
    # check which exchanges are already in table if any
    sql = "SELECT name from %s;" % (exchanges_table)
    exchange_list_in = set(pd.read_sql_query(sql,con=con)['name'])
 
    
    # get top cryptocurrencies by market cap from coinmarketcap
    url = "https://api.coinmarketcap.com/v1/ticker"
    response = requests.get(url)
    data = response.json()
    
    symbol_list = pd.DataFrame({'symbol':[d['symbol'] for d in data], 'market_cap_usd':[d['market_cap_usd'] for d in data]})
    symbol_list['market_cap_usd'] = pd.to_numeric(symbol_list['market_cap_usd'])
    symbol_list = symbol_list.sort_values('market_cap_usd', ascending=False)
    symbol_list.reset_index(inplace=True, drop=True) 
    symbol_list = set(symbol_list.loc[0:(n_currencies-1),'symbol'])   # list of n_currencies with highest market capitalization in USD
    
    
    # do NOT only consider tickers which are NOT already there!!!
    # we need to check the top exchanges on which they are traded first, as there could be new top exchanges for cryptocurrencies that are already in the table
    # we would miss these cases if we only check for cryptocurrencies that are NOT already in the database

    
    symbol_list_cryptocompare = set(coin_info.keys())

    # when generating the reference list in 'define_crypto_ref_list' we kept all missing currencies and considered them as 'real' currencies
    # here we are only interested in available cryptocurrencies, so we drop the missing ones and show a warning
    cryptos = symbol_list & symbol_list_cryptocompare
    cryptos_missing = symbol_list - symbol_list_cryptocompare 
 
    if len(cryptos_missing)>0:
        print ("----------------------------------------------------------")
        print ("The following top " + str(n_currencies)+" cryptocurrencies (considering their market cap from \'coinmarketcap\') could not be found on \'cryptocompare\':")
        print (cryptos_missing)
        print ("They will be removed from the list of cryptocurrencies which are added to the database.")
        print ("----------------------------------------------------------")
    

    if len(cryptos)>0:   # could only be False if ALL top cryptocurrencies from 'coinmarketcap' cannot be found on 'cryptocompare' which is very unlikely
        # the following list will never be empty as the corresponding table was filled before in 'define_crypto_ref_list'
        sql = "SELECT ticker from cryptocurrencies_symbols_reference_currencies;"
        ref_list = list(pd.read_sql_query(sql, con=con)['ticker'])

    
        # for all remaining top cryptocurrencies find top exchanges by volumne on which they are traded against currencies in ref_list
        for i, symbol in enumerate(cryptos):
            for j, symbol_ref in enumerate(ref_list):
                if symbol!=symbol_ref:
                    url = 'https://min-api.cryptocompare.com/data/top/exchanges?fsym=%s&tsym=%s&limit=%s'% (symbol, symbol_ref, str(n_exchanges))
                    response = requests.get(url)
                    temp = response.json()['Data']
                    temp = [d['exchange'] for d in temp]
                    temp = temp + ['CCCAGG']    # add average over all exchanges (similar to what we get from Yahoo for equities where we can only get average stock prices)
                    temp  = pd.DataFrame({'currency':[symbol] * len(temp),'ref_currency':[symbol_ref] * len(temp),'exchange':temp})
                    all_combinations = all_combinations.append(temp, ignore_index=True)
                    
        exchange_list = set(all_combinations['exchange'])
        symbol_list = set(all_combinations['currency'])
    
    
        # prepare list of symbols to add to database
        symbol_list = symbol_list - symbol_list_in   # only consider symbols which are not already there
        if len(symbol_list)>0:
            for i, ticker in enumerate(cryptos):    
                TotalCoinSupply = coin_info[ticker]['TotalCoinSupply']
                if TotalCoinSupply=='N/A':
                    TotalCoinSupply = None
                symbols.append( ( ticker, 'cryptocurrency', coin_info[ticker]['CoinName'], coin_info[ticker]['ProofType'], TotalCoinSupply, now ) )


        # prepare list of exchanges to add to database
        exchange_list = exchange_list - exchange_list_in   # only consider exchanges which are not already there
        if len(exchange_list)>0:  
            for i, exchange in enumerate(exchange_list):        
                exchanges.append( ( exchange, exchange, now ) )
                
    return symbols, symbols_columns, symbols_table, exchanges, exchanges_columns, exchanges_table, all_combinations



# convert all_combinations from names to keys
def def_all_combinations(all_combinations):
    
    all_combinations['vendor'] = 'cryptocompare'    # hardcoded

    sql = "SELECT id as cryptocurrencies_data_vendors_id, name as vendor from cryptocurrencies_data_vendors;"
    vendor = pd.read_sql_query(sql, con=con)
    all_combinations = pd.merge(all_combinations, vendor, on='vendor', how='left')
    
    sql = "SELECT id as cryptocurrencies_exchanges_id, name as exchange from cryptocurrencies_exchanges;"
    exchange = pd.read_sql_query(sql, con=con)
    all_combinations = pd.merge(all_combinations, exchange, on='exchange', how='left')

    sql = "SELECT id as cryptocurrencies_symbols_id, ticker as currency from cryptocurrencies_symbols;"
    symbol = pd.read_sql_query(sql, con=con)
    all_combinations = pd.merge(all_combinations, symbol, on='currency', how='left')

    sql = "SELECT id as cryptocurrencies_symbols_reference_currencies_id, ticker as ref_currency from cryptocurrencies_symbols_reference_currencies;"
    symbol_ref = pd.read_sql_query(sql, con=con)
    all_combinations = pd.merge(all_combinations, symbol_ref, on='ref_currency', how='left')
    
    
   
    # load combinations which are currenctly in the table and keep only new rows
    sql = "SELECT cryptocurrencies_data_vendors_id, cryptocurrencies_exchanges_id, cryptocurrencies_symbols_id, cryptocurrencies_symbols_reference_currencies_id from cryptocurrencies_combinations;"
    all_combinations_in = pd.read_sql_query(sql,con=con)
    all_combinations_in['check'] = 1
    
    all_combinations = pd.merge(all_combinations, all_combinations_in, on=['cryptocurrencies_data_vendors_id','cryptocurrencies_exchanges_id','cryptocurrencies_symbols_id','cryptocurrencies_symbols_reference_currencies_id'], how='left')
    all_combinations = all_combinations[pd.isnull(all_combinations['check'])]    # only those are not yet in the table

    all_combinations = all_combinations.drop(['currency','exchange','ref_currency','vendor','check'], axis=1)
    

    columns = all_combinations.columns.tolist()  + ['created_date']
    table = 'cryptocurrencies_combinations'

    all_combinations = all_combinations.values.tolist()
    all_combinations = [(tuple(l + [now])) for l in all_combinations]
    
    return all_combinations, columns, table




# fill all tables
if __name__ == "__main__":
    
    # get current date to add timestamp to every entry in the database
    now = datetime.datetime.utcnow()    

    # get information on all cryptocurrencies on cryptocompare
    coin_info = get_coin_info()
    
    # prepare list of vendors (currently only 'cryptocompare') and write to database
    vendors = define_vendor()
    insert_into_table(vendors,con)
   
    # prepare list of reference currencies and write to database
    # we consider trading of cryptocurrencies against these reference currencies
    symbols = define_crypto_ref_list(ref_list, coin_info)
    insert_into_table(symbols,con)
    
    # prepare list of top cryptocurrencies and top exchanges of interest and write to database
    # in addition generate all combinations of interest for the triplet ()
    symbols_exchanges_combinations = get_top_cryptocurrencies_exchanges(n_currencies, n_exchanges, coin_info)
    
    symbols = symbols_exchanges_combinations[0:3]
    insert_into_table(symbols,con)
    
    exchanges = symbols_exchanges_combinations[3:6]
    insert_into_table(exchanges,con)
    
    all_combinations = symbols_exchanges_combinations[6]
    all_combinations = def_all_combinations(all_combinations)
    insert_into_table(all_combinations,con)


con.close()





