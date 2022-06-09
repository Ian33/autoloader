# -*- coding: utf-8 -*-
"""
Created on Mon Jan 31 17:02:32 2022

@author: IHiggins
"""


import pyodbc
import pandas as pd
from sqlalchemy import create_engine
import urllib

conn = pyodbc.connect('Driver={SQL Server};'
                          'Server=KCITSQLDEVNRP01;'
                          'Database=gData;'
                          'Trusted_Connection=yes;')
'''
Gage_Lookup = pd.read_sql_query('select G_ID, SITE_CODE from tblGaugeLLID;', conn)
print(Gage_Lookup)

existing_data = conn.execute("select max(D_TimeDate) from tblDischargeGauging WHERE G_ID = "+str(1679)+";").fetchval()
conn.execute('delete from tblDischargeGauging WHERE G_ID = '+str(1679)+' AND D_TimeDate between ? and ?', '10/1/2021','2/1/2022')
conn.commit()
print("last data pre cut "+str(existing_data))
data = pd.read_sql_query('select D_TimeDate, D_Discharge from tblDischargeGauging WHERE G_ID = '+str(1679),conn)
print(data)
conn.close()
'''
#table = "tblConductivityGageRun"
server = "KCITSQLPRNRPX01"
driver = "SQL Server"
database = "gData"
trusted_connection = "yes"
    #pyodbc_string = 'Driver={'+driver+'};Server='+server+';Database='+database+';Trusted_Connection='+trusted_connection+';'
conn = pyodbc.connect('Driver={'+driver+'};'
                      'Server='+server+';'
                      'Database='+database+';'
                      'Trusted_Connection='+trusted_connection+';')
                       
sql_alchemy_connection = urllib.parse.quote_plus('DRIVER={'+driver+'}; SERVER='+server+'; DATABASE='+database+'; Trusted_Connection='+trusted_connection+';')
sql_engine = create_engine("mssql+pyodbc:///?odbc_connect=%s" % sql_alchemy_connection)
cnxn = sql_engine.raw_connection()

cnxn.close()
                       
sql_alchemy_connection = urllib.parse.quote_plus('DRIVER={'+driver+'}; SERVER='+server+'; DATABASE='+database+'; Trusted_Connection='+trusted_connection+';')
sql_engine = create_engine("mssql+pyodbc:///?odbc_connect=%s" % sql_alchemy_connection)


#table = "tblConductivityGageRun"
table = "tblDOGageRun"
 # create data for insert
'''
G_ID = 2162
Run_By = 1
Date_Run = "5/25/2022"
File_Name = "33DN_Table1.txt"
Instrument_Offset = 0
Column_Number = 6
UTC_Offset = 0
NumberOfHeaderRows = 3
ParameterID = 5 

d = {'G_ID': [G_ID], 
     'Run_By': [Run_By], 
     'Date_Run': [Date_Run], 
     'File_Name': [File_Name], 
     'Instrument_Offset': [Instrument_Offset], 
     'Column_Number': [Column_Number], 
     'UTC_Offset': [UTC_Offset],
     'NumberOfHeaderRows': [NumberOfHeaderRows],
     'ParameterID': [ParameterID]}

'''
#table = "tblDOGageRun"
#Run_DO_ID:
#G_ID: 2162
#Run_By: 1
#Date_Run: 5/25/2022
#File_Name: 33DN_Table1.txt
#Instrument_Offset: 0
#Column_Number: 6
#UTC_Offset: 0
#NumberOfHeaderRows: 3
#ParameterID: 5

#33DN_Table1.txt
'''
df = pd.DataFrame(data=d)
'''
#print(df)

sql_alchemy_connection = urllib.parse.quote_plus('DRIVER={'+driver+'}; SERVER='+server+'; DATABASE='+database+'; Trusted_Connection='+trusted_connection+';')
sql_engine = create_engine("mssql+pyodbc:///?odbc_connect=%s" % sql_alchemy_connection)
cnxn = sql_engine.raw_connection()
#df.to_sql(table, sql_engine, method=None, if_exists='append', index=False)
# try method=multi, None works
# try chunksize int

cnxn.close()
'''
result = sql_engine.execute('SELECT * FROM '
                        '"tblConductivityGageRun"')

names = sql_engine.table_names()
print(result)
#connection = sql_engine.connect()
#results = connection.execute(stmt).fetchall()
'''
existing_data = pd.read_sql_query(f'select * from {table};', conn)
# uploaded table
print("existing_data")
print(existing_data)
