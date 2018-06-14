#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Jun  7 03:22:24 2018

@author: SebastianArr
"""




# insert 'data' into table
# data must be a list of length 3
# data[0]: actual data
# data[1]: column names
# data[2]: table name
def insert_into_table(data, con):
    table = data[2]
    columns = data[1]
    data = data[0]

    if len(data)>0:
        column_str = ','.join(columns)
        insert_str = ("%s," * len(columns))[:-1]
        final_str = "INSERT INTO " + table + " (%s) VALUES (%s)" % (column_str, insert_str)
    
        cur = con.cursor()
        cur.executemany(final_str, data)
        con.commit()
        cur.close()

        print("%s entries were successfully added to table \'%s\'." % (len(data),table))
    else:
        print("No entries were added to table \'%s\'." % (table))
