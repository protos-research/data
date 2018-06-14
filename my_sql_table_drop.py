#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Apr 25 23:58:25 2018

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
TABLES = ['equities_data_vendors','equities_exchanges','equities_symbols','equities_combinations','equities_daily_prices',
          'cryptocurrencies_data_vendors','cryptocurrencies_exchanges','cryptocurrencies_symbols',
          'cryptocurrencies_symbols_reference_currencies','cryptocurrencies_combinations','cryptocurrencies_daily_prices']



# check if this is really wanted! --> don't delete tables by accident
print("You are going to drop the following tables:")
print (TABLES)
confirmation = input('If you are sure about this type \'yes\': ')



# drop required tables
if confirmation=='yes':
    for name in TABLES:
        try:
            print("Drop table: ", name)
            cur = con.cursor()
            cur.execute("SET FOREIGN_KEY_CHECKS = 0;")
            cur.execute("Drop TABLE `%s`" % (name))
            cur.execute("SET FOREIGN_KEY_CHECKS = 1;")
            cur.close()
        except mdb.Error as err:
            if err.errno == mdb.errorcode.ER_BAD_TABLE_ERROR:
                print("Warning: Table does not exist. It cannot be dropped!")
            else:
                print(err.msg)
        else:
            print("OK")
else:
    print('Dropping of tables aborted.')
    


con.close()



