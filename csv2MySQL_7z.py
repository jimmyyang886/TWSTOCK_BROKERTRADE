#!/usr/bin/env python
# coding: utf-8

# # Last Date Check

# In[1]:


# -*- coding: utf-8 -*-
import pandas as pd
from os import walk
import csv
import re
import sqlalchemy
from sqlalchemy import create_engine


engine = create_engine("mysql+pymysql://teb101Club:teb101Club@localhost/twstock??charset=utf8mb4", max_overflow=5)

#query = "select distinct date from broker_transaction;"
query = "select max(date) from broker_transaction;"

latestDate=list(engine.execute(query))[0][0]

#latestDate=max(datelist)[0].isoformat()
# for _date in datelist:
#     dateisolist.append(_date[0].isoformat())
#     print(_date[0].isoformat())
print(latestDate)


# In[2]:


import io
import os
import py7zlib

class SevenZFileError(py7zlib.ArchiveError):
    pass

class SevenZFile(object):
    @classmethod
    def is_7zfile(cls, filepath):
        """ Determine if filepath points to a valid 7z archive. """
        is7z = False
        fp = None
        try:
            fp = open(filepath, 'rb')
            archive = py7zlib.Archive7z(fp)
            n = len(archive.getnames())
            is7z = True
        finally:
            if fp: fp.close()
        return is7z

    def __init__(self, filepath):
        fp = open(filepath, 'rb')
        self.filepath = filepath
        self.archive = py7zlib.Archive7z(fp)

    def __contains__(self, name):
        return name in self.archive.getnames()

    def readlines(self, name):
        """ Iterator of lines from an archive member. """
        if name not in self:
            raise SevenZFileError('archive member %r not found in %r' %
                                  (name, self.filepath))

        for line in io.StringIO(self.archive.getmember(name).read().decode()):
            yield line


# In[ ]:


from datetime import datetime

import os

#import lzma


#mypath = "TWSE/"
mypath = "TWSE_7z/"


#file_path_list = []



def sqlcol(dfparam):    
    dtypedict = {}
    for i,j in zip(dfparam.columns, dfparam.dtypes):
        if "object"  in str(j):
            dtypedict.update({i: sqlalchemy.types.NVARCHAR(length=4)})

        if "datetime" in str(j):
            dtypedict.update({i: sqlalchemy.types.DATE()})

        if "float" in str(j):
            dtypedict.update({i: sqlalchemy.types.Float(precision=2, asdecimal=True)})

        if "int" in str(j):
            dtypedict.update({i: sqlalchemy.types.INT()})

        if "int64" in str(j):
            dtypedict.update({i: sqlalchemy.types.BIGINT()})

    return dtypedict

engine = create_engine("mysql+pymysql://teb101Club:teb101Club@127.0.0.1:3306/twstock??charset=utf8mb4", max_overflow=5)


for _directory in os.listdir(mypath):
    #print(_directory[:-3]) 
    #print(datetime_object)
    #print(datetime.strptime(_directory, '%Y-%m-%d').date())
    if datetime.strptime(_directory[:-3], '%Y-%m-%d').date() > latestDate:
#        for file in os.listdir(mypath+_directory):
#             print(file)        
        sevenZfile = SevenZFile(mypath+_directory)
        for file in sevenZfile.archive.getnames():
            if 'csv' in file:   
                whole_data = []
                rows = csv.reader(sevenZfile.readlines(file))

#                filepath=mypath+_directory+'/'+file
#                with open(filepath, newline='',encoding="utf-8") as csvfile:
#                rows = csv.reader(csvfile)
                    

                data = []
                for row in rows:
                    data.append(row[0:5])
                    data.append(row[6:11])
                
                #print(data)
                #print(data[2])
                #ticker = re.search(r"[\d]+",data[4][1]).group(0)
                ticker = re.search(r"[\d]+",data[2][1]).group(0)
                date = datetime.strptime(_directory[:-3], '%Y-%m-%d').date() 
                
                for i in data:
                    i.append(date)
                    i.append(ticker)

                #whole_data.extend(data[12:])
                whole_data.extend(data[6:])

                column = ['sn', 'bID', 'price', 'buy','sell','Date','sID']
                df = pd.DataFrame(whole_data, columns = column)

                #print(df)
                #print(f"Current Total Data: {df.info()}")
                #df.info()

                # print(df[df["sn"].isna()])                    
                df['bID'] = df["bID"].apply(lambda x: x[:4])    



                df["sn"] = df[df["sn"] != '']
                df = df.dropna()

                List_df = df[['bID','sID','Date','sn', 'price', 'buy','sell']]
                List_df[["Date"]] = List_df[["Date"]].astype('datetime64[ns]')
                List_df[["sn"]] = List_df[["sn"]].astype('int')
                List_df[["buy"]] = List_df[["buy"]].astype('int')
                List_df[["sell"]] = List_df[["sell"]].astype('int')
                List_df[["price"]] = List_df[["price"]].astype('float') 
                #List_df.info()
                #print(List_df)

                outputdict = sqlcol(List_df)

                print('import mysql', date, ticker)
                List_df.to_sql('broker_transaction', engine, schema='twstock',if_exists='append',index= False, dtype=outputdict, chunksize = 1000)


# In[ ]:




