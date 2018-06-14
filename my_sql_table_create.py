#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Apr 25 23:05:09 2018

@author: SebastianArr
"""


import mysql.connector as mdb
import getpass


# Connect to the MySQL instance
db_host = 'localhost'
db_user = 'root'
db_name = 'securities_master'
db_pass = getpass.getpass('Enter database password: ')

con = mdb.connect(host=db_host, user=db_user, passwd=db_pass, db=db_name)



# create dictionary with needed tables
TABLES = {}


# define tables for equities
TABLES['equities_data_vendors'] = (
  "CREATE TABLE `equities_data_vendors` ("
  "`id` int NOT NULL AUTO_INCREMENT,"
  "`name` varchar(64) NOT NULL,"
  "`website_url` varchar(255) NULL,"
  "`support_email` varchar(255) NULL,"
  "`created_date` datetime NOT NULL,"
  "PRIMARY KEY (`id`)"
  ") ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8;")

TABLES['equities_exchanges'] = (
  "CREATE TABLE `equities_exchanges` ("
  "`id` int NOT NULL AUTO_INCREMENT,"
  "`abbreviation` varchar(32) NOT NULL,"
  "`name` varchar(255) NOT NULL,"
  "`city` varchar(255) NULL,"
  "`country` varchar(255) NULL,"
  "`currency` varchar(64) NULL,"
  "`timezone_offset` time NULL,"
  "`created_date` datetime NOT NULL,"
  "PRIMARY KEY (`id`)"
  ") ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8;")

TABLES['equities_symbols'] = (
  "CREATE TABLE `equities_symbols` ("
  "`id` int NOT NULL AUTO_INCREMENT,"
  "`ticker` varchar(32) NOT NULL,"
  "`instrument` varchar(64) NOT NULL,"
  "`name` varchar(255) NULL,"
  "`sector` varchar(255) NULL,"
  "`currency` varchar(32) NULL,"
  "`created_date` datetime NOT NULL,"
  "PRIMARY KEY (`id`)"
  ") ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8;")

TABLES['equities_combinations'] = (
  "CREATE TABLE `equities_combinations` ("
  "`id` int NOT NULL AUTO_INCREMENT,"
  "`equities_data_vendors_id` int NOT NULL,"
  "`equities_exchanges_id` int NOT NULL,"
  "`equities_symbols_id` int NOT NULL,"
  "`created_date` datetime NOT NULL,"
  "PRIMARY KEY (`id`),"
  "FOREIGN KEY (`equities_data_vendors_id`) REFERENCES equities_data_vendors (`id`) ON DELETE CASCADE ON UPDATE CASCADE,"
  "FOREIGN KEY (`equities_exchanges_id`) REFERENCES equities_exchanges (`id`) ON DELETE CASCADE ON UPDATE CASCADE,"
  "FOREIGN KEY (`equities_symbols_id`) REFERENCES equities_symbols (`id`) ON DELETE CASCADE ON UPDATE CASCADE"
  ") ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8;")

TABLES['equities_daily_prices'] = (
  "CREATE TABLE `equities_daily_prices` ("
  "`id` int NOT NULL AUTO_INCREMENT,"
  "`equities_combinations_id` int NOT NULL,"
  "`price_date` datetime NOT NULL,"
  "`created_date` datetime NOT NULL,"
  "`open_price` decimal(19,4) NULL,"
  "`high_price` decimal(19,4) NULL,"
  "`low_price` decimal(19,4) NULL,"
  "`close_price` decimal(19,4) NULL,"
  "`close_price_adjusted` decimal(19,4) NULL,"
  "`volume` bigint NULL,"
  "PRIMARY KEY (`id`),"
  "FOREIGN KEY (`equities_combinations_id`) REFERENCES equities_combinations (`id`) ON DELETE CASCADE ON UPDATE CASCADE"
  ") ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8;")


# define tables for cryptocurrencies
TABLES['cryptocurrencies_data_vendors'] = (
  "CREATE TABLE `cryptocurrencies_data_vendors` ("
  "`id` int NOT NULL AUTO_INCREMENT,"
  "`name` varchar(64) NOT NULL,"
  "`website_url` varchar(255) NULL,"
  "`support_email` varchar(255) NULL,"
  "`created_date` datetime NOT NULL,"
  "PRIMARY KEY (`id`)"
  ") ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8;")

TABLES['cryptocurrencies_exchanges'] = (
  "CREATE TABLE `cryptocurrencies_exchanges` ("
  "`id` int NOT NULL AUTO_INCREMENT,"
  "`abbreviation` varchar(32) NOT NULL,"
  "`name` varchar(255) NOT NULL,"
  "`city` varchar(255) NULL,"
  "`country` varchar(255) NULL,"
  "`currency` varchar(64) NULL,"
  "`timezone_offset` time NULL,"
  "`created_date` datetime NOT NULL,"
  "PRIMARY KEY (`id`)"
  ") ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8;")

TABLES['cryptocurrencies_symbols'] = (
  "CREATE TABLE `cryptocurrencies_symbols` ("
  "`id` int NOT NULL AUTO_INCREMENT,"
  "`ticker` varchar(32) NOT NULL,"
  "`instrument` varchar(64) NOT NULL,"
  "`name` varchar(255) NULL,"
  "`proof_type` varchar(255) NULL,"
  "`total_coin_supply` bigint NULL,"
  "`created_date` datetime NOT NULL,"
  "PRIMARY KEY (`id`)"
  ") ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8;")

TABLES['cryptocurrencies_symbols_reference_currencies'] = (
  "CREATE TABLE `cryptocurrencies_symbols_reference_currencies` ("
  "`id` int NOT NULL AUTO_INCREMENT,"
  "`ticker` varchar(32) NOT NULL,"
  "`instrument` varchar(64) NOT NULL,"
  "`name` varchar(255) NULL,"
  "`proof_type` varchar(255) NULL,"
  "`total_coin_supply` bigint NULL,"
  "`created_date` datetime NOT NULL,"
  "PRIMARY KEY (`id`)"
  ") ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8;")

TABLES['cryptocurrencies_combinations'] = (
  "CREATE TABLE `cryptocurrencies_combinations` ("
  "`id` int NOT NULL AUTO_INCREMENT,"
  "`cryptocurrencies_data_vendors_id` int NOT NULL,"
  "`cryptocurrencies_exchanges_id` int NOT NULL,"
  "`cryptocurrencies_symbols_id` int NOT NULL,"
  "`cryptocurrencies_symbols_reference_currencies_id` int NOT NULL,"
  "`created_date` datetime NOT NULL,"
  "PRIMARY KEY (`id`),"
  "FOREIGN KEY (`cryptocurrencies_data_vendors_id`) REFERENCES cryptocurrencies_data_vendors (`id`) ON DELETE CASCADE ON UPDATE CASCADE,"
  "FOREIGN KEY (`cryptocurrencies_exchanges_id`) REFERENCES cryptocurrencies_exchanges (`id`) ON DELETE CASCADE ON UPDATE CASCADE,"
  "FOREIGN KEY (`cryptocurrencies_symbols_id`) REFERENCES cryptocurrencies_symbols (`id`) ON DELETE CASCADE ON UPDATE CASCADE,"
  "FOREIGN KEY (`cryptocurrencies_symbols_reference_currencies_id`) REFERENCES cryptocurrencies_symbols_reference_currencies (`id`) ON DELETE CASCADE ON UPDATE CASCADE"
  ") ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8;")

TABLES['cryptocurrencies_daily_prices'] = (
  "CREATE TABLE `cryptocurrencies_daily_prices` ("
  "`id` int NOT NULL AUTO_INCREMENT,"
  "`cryptocurrencies_combinations_id` int NOT NULL,"
  "`price_date` datetime NOT NULL,"
  "`created_date` datetime NOT NULL,"
  "`open_price` decimal(19,4) NULL,"
  "`high_price` decimal(19,4) NULL,"
  "`low_price` decimal(19,4) NULL,"
  "`close_price` decimal(19,4) NULL,"
  "`volume_from` decimal(25,2) NULL,"
  "`volume_to` decimal(25,2) NULL,"
  "PRIMARY KEY (`id`),"
  "FOREIGN KEY (`cryptocurrencies_combinations_id`) REFERENCES cryptocurrencies_combinations (`id`) ON DELETE CASCADE ON UPDATE CASCADE"
  ") ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8;")



# create required tables
for name, ddl in TABLES.items():
    try:
        print("Creating table: ", name)
        cur = con.cursor()
        cur.execute(ddl)
        cur.close()
    except mdb.Error as err:
        if err.errno == mdb.errorcode.ER_TABLE_EXISTS_ERROR:
            print("Warning: Table already exists. It is not overwritten.")
        else:
            print(err.msg)
    else:
        print("OK")

con.close()




