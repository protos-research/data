#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Apr 25 23:05:09 2018

@author: SebastianArr
"""

import datetime
import getpass
import mysql.connector as mdb
import bs4
import pandas as pd
import requests
from auxiliary_functions import insert_into_table


# Connect to the MySQL instance
db_host = 'localhost'
db_user = 'root'
db_name = 'securities_master'
db_pass = getpass.getpass('Enter database password: ')
con = mdb.connect(host=db_host, user=db_user, passwd=db_pass, db=db_name)




# create list of vendors
# only use 'yahoo finance' (hardcoded)
def define_vendor():
    vendors = []
    columns = ['name','website_url','created_date']
    table = 'equities_data_vendors'
        
    # check which vendors are already in table if any
    sql = "SELECT name from %s;" % (table)
    vendor_list_in = set(pd.read_sql_query(sql,con=con)['name'])

    vendor_list = set(['yahoo finance'])    # hardcoded
    vendor_list = vendor_list - vendor_list_in   # add only those which are not yet in

    if len(vendor_list)>0:
        vendors.append(('yahoo finance','https://finance.yahoo.com/',now))
        
    return vendors, columns, table



# create list of exchanges
# only use 'CCCAGG' (hardcoded) --> abbreviation for average (same as used on cryptocompare)
def define_exchange():
    exchanges = []
    columns = ['abbreviation','name','created_date']
    table = 'equities_exchanges'
       
    # check which vendors are already in table if any
    sql = "SELECT name from %s;" % (table)
    exchange_list_in = set(pd.read_sql_query(sql,con=con)['name'])

    exchange_list = set(['CCCAGG'])    # hardcoded
    exchange_list = exchange_list - exchange_list_in   # add only those which are not yet in

    if len(exchange_list)>0:
        exchanges.append(('CCCAGG','CCCAGG',now))
        
    return exchanges, columns, table



# get list of S&P 500 stocks from wikipedia
def get_SNP500_stocks():
    symbols = []
    symbols_columns = ['ticker','instrument','name','sector','currency','created_date']
    symbols_table = 'equities_symbols'


    # check which symbols are already in table if any
    sql = "SELECT ticker from %s;" % (symbols_table)
    symbol_list_in = set(pd.read_sql_query(sql,con=con)['ticker'])


    url = 'http://en.wikipedia.org/wiki/List_of_S%26P_500_companies'
    response = requests.get(url)
    symbol_list = bs4.BeautifulSoup(response.text, "lxml")
    symbol_list = symbol_list.select('table')[0].select('tr')[1:]

    symbols = []
    for i, symbol in enumerate(symbol_list):
        tds = symbol.select('td')
        ticker = tds[0].select('a')[0].text
        
        if ticker not in symbol_list_in:
            symbols.append( (ticker, 'stock', tds[1].select('a')[0].text, tds[3].text, 'USD', now ) )
            
    return symbols, symbols_columns, symbols_table




# get all_combinations --> easy for equities as at the moment we have only one vendor and one 'exchange' for equities
def def_all_combinations(symbols):
    
    all_combinations = pd.DataFrame({'symbol':symbols})
    all_combinations['exchange'] = 'CCCAGG'
    all_combinations['vendor'] = 'yahoo finance'

    sql = "SELECT id as equities_data_vendors_id, name as vendor from equities_data_vendors;"
    vendor = pd.read_sql_query(sql, con=con)
    all_combinations = pd.merge(all_combinations, vendor, on='vendor', how='left')
    
    sql = "SELECT id as equities_exchanges_id, name as exchange from equities_exchanges;"
    exchange = pd.read_sql_query(sql, con=con)
    all_combinations = pd.merge(all_combinations, exchange, on='exchange', how='left')

    sql = "SELECT id as equities_symbols_id, ticker as symbol from equities_symbols;"
    symbol = pd.read_sql_query(sql, con=con)
    all_combinations = pd.merge(all_combinations, symbol, on='symbol', how='left')
    
    
    
    # load combinations which are currenctly in the table and keep only new rows
    sql = "SELECT equities_data_vendors_id, equities_exchanges_id, equities_symbols_id from equities_combinations;"
    all_combinations_in = pd.read_sql_query(sql,con=con)
    all_combinations_in['check'] = 1
    
    all_combinations = pd.merge(all_combinations, all_combinations_in, on=['equities_data_vendors_id','equities_exchanges_id','equities_symbols_id'], how='left')
    all_combinations = all_combinations[pd.isnull(all_combinations['check'])]    # only those are not yet in the table

    all_combinations = all_combinations.drop(['symbol','exchange','vendor','check'], axis=1)
    

    columns = all_combinations.columns.tolist()  + ['created_date']
    table = 'equities_combinations'

    all_combinations = all_combinations.values.tolist()
    all_combinations = [(tuple(l + [now])) for l in all_combinations]
    
    return all_combinations, columns, table




# fill all tables
if __name__ == "__main__":
    
    # get current date to add timestamp to every entry in the database
    now = datetime.datetime.utcnow()    

    # prepare list of vendors (currently only 'yahoo') and write to database
    vendors = define_vendor()
    insert_into_table(vendors,con)

    # prepare list of exchanges (currently only 'CCCAGG') and write to database
    exchanges = define_exchange()
    insert_into_table(exchanges,con)

    # prepare list of S&P 500 stocks and write to database
    symbols = get_SNP500_stocks()
    insert_into_table(symbols,con)

    # prepare list of all needed combinations and write to database
    symbols = symbols[0]
    symbols = [d[0] for d in symbols]
 
    all_combinations = def_all_combinations(symbols)
    insert_into_table(all_combinations,con)


con.close()


