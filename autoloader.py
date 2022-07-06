# -*- coding: utf-8 -*-
"""
Created on Mon Aug 30 10:02:36 202
@author: IHiggins, RHiggins
"""
#
# you need to add an IntegrityError: skip that if this occurs jsut skip and try again
import sys
import os
from datetime import datetime
from datetime import timedelta
import urllib
import configparser
import time
import numpy as np
import win32com.client as win32
import schedule
import pyodbc
import pandas as pd
import os
import os.path
#import sqlalchemy
from sqlalchemy import create_engine
from contextlib import redirect_stdout
#from sqlalchemy.engine import URL
pd.options.mode.chained_assignment = None  # default='warn', None is no warn
#### TEST BRANCH COMMENT #####
### SECOND TEST BRANCH COMMET ####
# import time
# import datetime
# from time import time, sleep
# import time

# thing to run
# from apscheduler.schedulers.blocking import BlockingScheduler
# get sql_parameters
config = configparser.ConfigParser()

config.read('gdata_config.ini')
# get access information
# save .ini to local documents, this is not the easiest method or the safest but its better then nothing
# program can look in a few places for file
# get user name for local drive
user = os.getlogin()

access = configparser.ConfigParser()

def get_access(file_name, path):
    
    if os.path.exists(f"{path}/{file_name}.ini") == True:
        access.read(f"{path}/{file_name}.ini")
        return access
    else:
        pass
file_name = "access"
path = ""

get_access(file_name, path)
path = f"C:/Users/{user}/Documents"
get_access(file_name, path)
path = f"C:/Users/{user}/OneDrive - King County"
get_access(file_name, path)

try: # search directery
    access.read('access.ini')
except:
    try: # search one drive 'my documents'
        access.read(f"C:/Users/{user}/OneDrive - King County/access.ini")
    except:
        try: # cearch windows documents
            access.read(f"C:/Users/{user}/Documents/access.ini")
            
        except:
            print("no access file found")


def run_upload():
    # this works but will update the file from run X at the start of run Y
    # to see terminal scielence this
    #orig_stdout = sys.stdout
    #f = open("W:\STS\hydro\GAUGE\zTelemetered_Tables\Autoloader_Output.txt", 'w')
    #sys.stdout = f
    
    #sys.stdout = open("W:\STS\hydro\GAUGE\zTelemetered_Tables\Autoloader_Output.txt", 'w')
    print("Run Start at "+str(pd.to_datetime('today'))+"")
    print("")
    server = access["sql_connection"]["server"]
    driver = access["sql_connection"]["driver"]
    database = access["sql_connection"]["database"]
    trusted_connection = access["sql_connection"]["trusted_connection"]
    conn = pyodbc.connect('Driver={'+driver+'};'
                          'Server='+server+';'
                          'Database='+database+';'
                          'Trusted_Connection='+trusted_connection+';')
                       
    # object calling is different then object interpolation
    # a variable defined before a function is global for function
    #sql_alchemy_connection = urllib.parse.quote_plus('DRIVER={'+driver+'}; SERVER='+server+'; DATABASE='+database+', Trusted_Connection='+trusted_connection)
    # DEV Server  KCITSQLDEVNRP01
    # Data server KCITSQLPRNRPX01
    gage_lookup = pd.read_sql_query(
        'select G_ID, SITE_CODE from tblGaugeLLID;', conn)

    # Funtion to send an email
    def e_mail():
        outlook = win32.Dispatch('outlook.application')
        mail = outlook.CreateItem(0)
        mail.To = 'ihiggins@kingcounty.gov'
        mail.Subject = 'Dev Autoloader Run'
        mail.Body = 'Run Successful This is an automatic email; to stop tell Ian to bug-off'
        # Information to send an attachment
        # mail.HTMLBody = '<h2>HTML Message body</h2>' #this field is optional
        # To attach a file to the email (optional):
        # attachment  = "W:\STS\hydro\GAUGE\zTelemetered_Tables\Autoloader_Output_Dev.txt"
        # mail.Attachments.Add(attachment)
        # TIME = pd.to_datetime('today')
        # sys.stdout = f
        # print "test"
        # f.close()
        mail.Send()
        print("")

    def get_reference_table(parameter, file_path, telemetry_table):
        ''' Returns the approperate telemetry table for parameter type'''
        # some sites have an offset so this tries both syles of talbe
        try:
            reference_table = pd.read_sql_query(f"select G_ID, {config[parameter]['telemetry_file']}, Column_Number, UTC_Offset, NumberOfHeaderRows, Instrument_Offset from {telemetry_table}", conn)
        except Exception:
            try:
                reference_table = pd.read_sql_query(f"select G_ID, {config[parameter]['telemetry_file']}, Column_Number, UTC_Offset, NumberOfHeaderRows from {telemetry_table}", conn)
            except Exception:
                reference_table = pd.read_sql_query(f"select G_ID, {config[parameter]['telemetry_file']}, Column_Number, NumberOfHeaderRows from {telemetry_table}", conn)
        return reference_table

    def get_site_name(reference_table):
        reference_table['G_ID'] = reference_table['G_ID'].astype('int64')
        site = reference_table.iloc[index, 0]
        search = gage_lookup.loc[gage_lookup['G_ID'].isin([site])]
        site_name = search.iloc[0, 1]
        return site_name

    # Try to open file with different methods
    def open_file(parameter, site_name, site_sql_id, file_path, file_name):
        '''Opens specific site files listed in reference table (tele table)'''
        try:
            parameter_upload_data = pd.read_csv(
                file_path+str("\\")+str(file_name),
                header=number_of_header_rows,
                usecols=[0, column_number])
            return parameter_upload_data
        except FileNotFoundError:
            print(str(parameter)+" "+str(site_name)+" ["+str(site_sql_id)+"] File is not present")
            parameter_upload_data = pd.DataFrame()
            return parameter_upload_data
        except OSError:
            print(str(parameter)+" "+str(site_name)+" ["+str(site_sql_id)+"] OS Error, invalid file path")
            parameter_upload_data = pd.DataFrame()
            return parameter_upload_data
        except ValueError:
            try:
                with open(file_path+str("\\")+str(file_name), encoding="utf8", errors='ignore') as f:
                    parameter_upload_data = pd.read_csv(f, header=number_of_header_rows, usecols=[0, column_number], engine='python', on_bad_lines='skip')
                return parameter_upload_data
            except Exception:
                print(str(parameter)+" "+str(site_name)+" ["+str(site_sql_id)+"] Value Error, columns do not match file size")
                parameter_upload_data = pd.DataFrame()
                return parameter_upload_data
        except:  # will try different methods before passing
            try:
                print("ISO-8859-1")
                parameter_upload_data = pd.read_csv(file_path+str('\\')+str(file_name), header=number_of_header_rows, usecols=[0, column_number], encoding="ISO-8859-1")
                return parameter_upload_data
            except Exception:
                try:
                    parameter_upload_data = pd.read_csv(file_path+str('\\')+str(file_name), header=number_of_header_rows, usecols=[0, column_number], encoding="cp1252")
                    print("cp1252")
                    return parameter_upload_data
                except Exception:
                    print(str(parameter)+" "+str(site_name)+" ["+str(site_sql_id)+"] Unknown File Load Error")
                    pass

    def clean_file(parameter_upload_data, offset, parameter, site_name, site_sql_id, utc_offset):
        '''Takes opened telemetry file and cleans data for processing'''
        pre_drop = parameter_upload_data.shape[1]
        #  drops values
        #  treats -inf as NA
        pd.options.mode.use_inf_as_na = True
        parameter_upload_data.replace('"NAN"', "NA", inplace=True)
        parameter_upload_data.dropna(inplace=True)
        post_drop = parameter_upload_data.shape[1]
        drops = pre_drop-post_drop
        if drops > 5:
            print(f"{parameter} {site_name} [{site_sql_id}] Greater then 5 consecutive NAN rows droped")
        #  Rename columns - note the value column is dynamic based on what is provided by the SQL we only read the two columns so this index method is fine
        parameter_upload_data.rename(columns={parameter_upload_data.columns[0]: "datetime"}, inplace=True)
        parameter_upload_data["datetime"] = pd.to_datetime(parameter_upload_data["datetime"])
        parameter_upload_data["datetime"] = parameter_upload_data["datetime"] + timedelta(hours=(utc_offset).astype(float))
        parameter_upload_data.rename(columns={parameter_upload_data.columns[1]: "data"}, inplace=True)
        # Round the 'value' column and convert data type
        parameter_upload_data['data'] = pd.to_numeric(parameter_upload_data['data'], errors='coerce').astype("float")
        parameter_upload_data['corrected_data'] = (parameter_upload_data['data']+float(offset)).round(2)
        parameter_upload_data['G_ID'] = str(site_sql_id)
        
        parameter_upload_data.drop_duplicates(subset=["datetime"], inplace=True)
        return parameter_upload_data

    def second_trip(parameter_upload_data):
        '''Blank holding for later'''
        # pd.options.mode.use_inf_as_na = True
        # parameter_upload_data.replace("", "NA", inplace=True)
        # parameter_upload_data.replace('"NAN"', "NA", inplace=True)
        # parameter_upload_data.dropna(inplace=True)
        return parameter_upload_data

    def cut_data(parameter, parameter_upload_data, site_sql_id):
        '''takes all imported telemetry data from telemetry file (clean_file)
           and removes data allready on server'''
        try:
            # attempt to trip data
            ##existing_data = cursor.execute("select max("+str(config[parameter]["datetime"])+") from "+str(config[parameter]["table"])+" WHERE G_ID = "+str(site_sql_id)+";").fetchval()
            #existing_data = cursor.execute(f"select max({config[parameter]['datetime']}) from {config[parameter]['table']} WHERE G_ID = {site_sql_id});").fetchval()
            existing_data = cursor.execute(f"select max({config[parameter]['datetime']}) from {config[parameter]['table']} WHERE G_ID = {site_sql_id};").fetchval()
            parameter_upload_data['datetime'] = pd.to_datetime(parameter_upload_data['datetime'])
            df = parameter_upload_data[parameter_upload_data['datetime'] > existing_data]
            return df
        except TypeError:
            # if there is no existing data, a TypeError is returned
            # upload all data
            parameter_upload_data['datetime'] = pd.to_datetime(parameter_upload_data['datetime'])
            df = parameter_upload_data
            return df
        except IndexError as e:
            # if there is an index error return blank
            print(e)
            print("Error finding old data, old data may not exist")
            df = pd.DataFrame({'A': [np.nan]})
            return df
            # pass
    '''
    def Time_Check(parameter, File_Path, Telemetry_Table, File_Name, CHECK_CSV, UTC_Offset, sql_prefix):
        # This takes the last value from RAW_parameter before it is modified and before the UTC OFFSET is applied
        CHECK_CSV[str(sql_prefix)+'TimeDate_UTC'] = CHECK_CSV[str(sql_prefix)+'TimeDate']+timedelta(hours=UTC_Offset)
        CHECK_CSV[str(sql_prefix)+'TimeDate_UTC'] = CHECK_CSV[str(sql_prefix)+'TimeDate_UTC'].dt.strftime('%Y-%m-%d %H:00')
        CHECK_CSV[str(sql_prefix)+'TimeDate_UTC'] = pd.to_datetime(CHECK_CSV[str(sql_prefix)+'TimeDate_UTC'])
        # This gets the UTC offset of the last value
        LAST_DATE_CSV_UTCOffset = CHECK_CSV[str(sql_prefix)+'TimeDate_UTC'].iloc[-1]
        LAST_DATE_CSV_UTCOffset = LAST_DATE_CSV_UTCOffset.strftime('%Y-%m-%d %H:%M')
        LAST_DATE_CSV_UTCOffset = datetime.strptime(LAST_DATE_CSV_UTCOffset, '%Y-%m-%d %H:00')
        RAW_DATE_CSV = CHECK_CSV[str(sql_prefix)+'TimeDate'].iloc[-1]
        RAW_DATE_CSV = RAW_DATE_CSV.strftime('%Y-%m-%d %H:00')

        # CALCULATE UTC OFFSET of computer yeah its this complicated
        NOW_UTC = datetime.utcnow()
        NOW_UTC = NOW_UTC.strftime('%Y-%m-%d %H:00')
        NOW_UTC = datetime.strptime(NOW_UTC, '%Y-%m-%d %H:00')
        NOW_REGIONAL = datetime.now()
        NOW_REGIONAL = NOW_REGIONAL.strftime('%Y-%m-%d %H:00')
        NOW_REGIONAL = datetime.strptime(NOW_REGIONAL, '%Y-%m-%d %H:00')
        PC_UTC_OFFSET = (NOW_UTC-NOW_REGIONAL)
        PC_UTC_OFFSET = PC_UTC_OFFSET
        PC_UTC_OFFSET = divmod(PC_UTC_OFFSET.seconds, 3600)
        PC_UTC_OFFSET = PC_UTC_OFFSET[0]

        # Adds an additional time zone offset if the computer is in PST
        if PC_UTC_OFFSET == 8:
            TimeZone_Offset = 1
          # Drop = 2
            TimeZone = "PST"
        else:
            TimeZone_Offset = 0
            TimeZone = "PDT"
        # Get modification date of CSV file
        stats = os.stat(File_Path+str("\\")+str(File_Name))
        # Gets modification time in different zones
        CSV_MODIFIED_PDT = (datetime.fromtimestamp(stats.st_mtime)+timedelta(hours=TimeZone_Offset)).strftime('%Y-%m-%d %H:00')
        CSV_MODIFIED_UTC = (datetime.fromtimestamp(stats.st_mtime)+timedelta(hours=PC_UTC_OFFSET)).strftime('%Y-%m-%d %H:00')
        CSV_MODIFIED_LOCAL = (datetime.fromtimestamp(stats.st_mtime)+timedelta(hours=0)).strftime('%Y-%m-%d %H:00')
        # converts time zone
        CSV_MODIFIED_PDT = datetime.strptime(CSV_MODIFIED_PDT, '%Y-%m-%d %H:00')
        CSV_MODIFIED_UTC = datetime.strptime(CSV_MODIFIED_UTC, '%Y-%m-%d %H:00')
        CSV_MODIFIED_LOCAL = datetime.strptime(CSV_MODIFIED_LOCAL, '%Y-%m-%d %H:00')

        DIFF = CSV_MODIFIED_UTC - LAST_DATE_CSV_UTCOffset
        # Check to see if last reporting time with correcting is greater then CSV modification date
        if LAST_DATE_CSV_UTCOffset > CSV_MODIFIED_UTC:
            print("")
            print(str(parameter)+" "+str(site_name)+" ["+str(site_sql_id)+"] Logger reporting time is greater then current time")
            print("Last CSV Date with UTC Offset of "+str(UTC_Offset)+" : "+str(LAST_DATE_CSV_UTCOffset))
            print("Current Time UTC "+str(NOW_UTC))
            print("")
        IS_OLD_DATA = "False"
        # CHeck to see if a logger is reporting out
        if LAST_DATE_CSV_UTCOffset <= (NOW_UTC - timedelta(hours=24)):
            print("")
            print(str(parameter)+" "+str(site_name)+" ["+str(site_sql_id)+"] Logger last reported "+str(NOW_UTC-LAST_DATE_CSV_UTCOffset))
            # if its older data it may not have updated since the time change so we will just ignore it for now
            IS_OLD_DATA = "True"
            print("")
        # Check UTC offset
        if LAST_DATE_CSV_UTCOffset != CSV_MODIFIED_UTC and IS_OLD_DATA == "False":
            print("")
            print(str(parameter)+" "+str(site_name)+" ["+str(site_sql_id)+"] Check UTC time on logger, and autoloader settings")
            print("Last CSV Record with UTC Offset of "+str(UTC_Offset)+" = "+str(LAST_DATE_CSV_UTCOffset))
            print("Date CSV Modified in Local time "+str(CSV_MODIFIED_LOCAL)+" Time Zone "+str(TimeZone))
            print("Date CSV Modified in UTC "+str(CSV_MODIFIED_UTC)+" ")
            print("Difference "+str(DIFF))
            if str(DIFF) == "7:00:00":
                print("Logger most likely set to PDT and Loading with a UTC_Offset of 0")
            print("")
    '''

    def upload_data(df, parameter, site_sql_id, utc_offset):
        '''takes data from cut_data, formats it for server and uploads
        different parameters will call different functions for individual
        sql tables as  defined in below if(parameter) statements'''

        def auto_timestamp_column():
            # time_now = pd.to_datetime('today')
            df[config[parameter]['auto_timestamp']] = pd.to_datetime('today')
            df[config[parameter]['auto_timestamp']] = df[config[parameter]['auto_timestamp']].dt.strftime('%m/%d/%Y %H:%M')
            return df

        def est_column():
            df[config[parameter]['est']] = "0"
            return df

        def depth_column():
            df[config[parameter]['depth']] = "0"
            return df

        def discharge_column():
            df.rename(columns={"discharge": config[parameter]['discharge']}, inplace=True)
            return df

        def dissolved_oxygen_precent_column():
            '''place holder for dissolved oxygen column'''
            return df

        # must be called after con
        # need to use config file for names
        def water_temperature_record_column():
            ''' place holder for dissolved oxygen column'''
            '''gets temperature from record'''
            '''returns df with a temperature column labled as the config water temperature'''
            start_time = df.head(1)
            start_time[config[parameter]['datetime']] = pd.to_datetime(
                start_time[config[parameter]['datetime']], format='%Y-%m-%d %H:%M:%S', errors='coerce', infer_datetime_format=True)
            start_time = start_time.iloc[0, 0]
            # get end time
            end_time = df.tail(1)
            end_time[config[parameter]['datetime']] = pd.to_datetime(
                end_time[config[parameter]['datetime']], format='%Y-%m-%d %H:%M:%S', errors='coerce', infer_datetime_format=True)
            end_time = end_time.iloc[0, 0]
            # since variables are imported into parent function you dont need to pass them to this
            # get temperature from record
            existing_data = pd.read_sql_query(f"select {config['water_temperature']['datetime']}, {config['water_temperature']['column']} from {config['water_temperature']['table']} WHERE G_ID = {site_sql_id} AND {config['water_temperature']['datetime']} between ? and ?;", conn, params=[str(start_time), str(end_time)])
            # convert df datetime to datetime
            df[config[parameter]['datetime']] = pd.to_datetime(
                df[config[parameter]['datetime']], format='%Y-%m-%d %H:%M:%S', errors='coerce', infer_datetime_format=True)
            # merge with autoloader df
            df_merged = pd.merge(df, existing_data, left_on=f"{config[parameter]['datetime']}", right_on = f"{config['water_temperature']['datetime']}")
            # drop water temperature datetime
            df_merged = df_merged.drop(columns=[f"{config['water_temperature']['datetime']}"])
            return df_merged

        def dissolved_oxygen_water_temperature_column():
            ''' renames water temperature column to comply with dissolved oxygen table'''
            # renames column
            df.rename(columns={f"{config['water_temperature']['column']}": f"{config[parameter]['temperature']}"}, inplace=True)
            return df

        def dissolved_oxygen_precent_column():
            # back of envelope method
            # https://www.umass.edu/mwwp/protocols/lakes/oxygen_lake.html
            # %DO saturation = your DO Measurement / Max. DO Concentration at tour measured temperature
            # calculate maximum concentration
            # maximum_concentration = -0.2444(temp_c) + 14.
            df[config[parameter]['precent']] = 0
            for index, row in df.iterrows():
                #print(df[config[parameter]['temperature']].loc[row])
                #print(row[config[parameter]['temperature']])
                #print(index)
                
                # calculate dissolved oxygen saturation
                
                df[config[parameter]['precent']].loc[index] = (df[config[parameter]['column']].loc[index]/((-0.2444*df[config[parameter]['temperature']].loc[index])+14))*100
                #row[config[parameter]['precent']] = -0.2444(row[config[parameter]['temperature']]) + 14.048
            #dissolved_oxygen_saturation = your DO Measurement / maximum_concentration
            return df

        def gallons_pumped_column():
            df[config[parameter]['gallons_pumped']] = ""
            return df

        def ice_column():
            df[config[parameter]['ice']] = "0"
            return df

        def lock_column():
            df[config[parameter]['lock']] = "0"
            return df

        def provisional_column():
            df[config[parameter]['provisional']] = "0"
            return df

        def pump_on_column():
            df[config[parameter]['pump_on']] = "0"
            return df

        def snow_column():
            df[config[parameter]['snow']] = "0"
            return df

        def site_id(site_sql_id):
            df["G_ID"] = str(site_sql_id)
            return df

        def sql_time():
            df[config[parameter]['datetime']] = df[config[parameter]['datetime']]
            df[config[parameter]['datetime']] = df[config[parameter]['datetime']] + timedelta(hours=(utc_offset).astype(float))
            df[config[parameter]['datetime']] = df[config[parameter]['datetime']].dt.strftime('%m/%d/%Y %H:%M:%S')
            return df

        def utc_offset_column(utc_offset):
            df[config[parameter]['utc_offset']] = str(utc_offset)
            return df

        def warning_column():
            df[config[parameter]['warning']] = "0"
            return df

        # The actual data upload
        def upload(df):
            try:
                '''function to actually upload data to server'''
                print(f"{parameter} {site_name} [{site_sql_id}] uploading")
                # sql_alchemy_connection = urllib.parse.quote_plus(f"DRIVER={{driver}}; SERVER={server}; DATABASE={database}, Trusted_Connection={trusted_connection}")
                sql_alchemy_connection = urllib.parse.quote_plus('DRIVER={'+driver+'}; SERVER='+server+'; DATABASE='+database+'; Trusted_Connection='+trusted_connection+';')
                sql_engine = create_engine("mssql+pyodbc:///?odbc_connect=%s" % sql_alchemy_connection)
                cnxn = sql_engine.raw_connection()
                df.to_sql(config[parameter]['table'], sql_engine, method=None, if_exists='append', index=False)
                # try method=multi, None works
                # try chunksize int
                print(f"{parameter} {site_name} [{site_sql_id}] inserted {df.shape[0]} records")
                cnxn.close()
            except:
                #raise
                pass

        df['datetime'] = pd.to_datetime(df['datetime'], format='%Y-%m-%d %H:%M:%S', errors='coerce', infer_datetime_format=True)
        # df["G_ID"] = str(site_sql_id)
        df.rename(columns={"datetime": config[parameter]['datetime']}, inplace=True)
        df.rename(columns={"data": config[parameter]['data']}, inplace=True)
        df.rename(columns={"corrected_data": config[parameter]['corrected_data']}, inplace=True)

        if parameter == "air_temperature":
            df = auto_timestamp_column()
            df = est_column()
            df = lock_column()
            df = provisional_column()
            df = site_id(site_sql_id)
            df = utc_offset_column(utc_offset)
            df = warning_column()
            # ONLY USE THIS FOR SQL IMPORT IT ADDS & HOURS
            df = sql_time()
            upload(df)

        if parameter == "barometer":
            df = auto_timestamp_column()
            df = est_column()
            df = lock_column()
            df = provisional_column()
            df = site_id(site_sql_id)
            df = utc_offset_column(utc_offset)
            df = warning_column()
            # ONLY USE THIS FOR SQL IMPORT IT ADDS & HOURS
            df = sql_time()
            upload(df)

        if parameter == "battery":
            df = site_id(site_sql_id)
            df = sql_time()
            df[config[parameter]['datetime']] = pd.to_datetime(df[config[parameter]['datetime']], format='%Y-%m-%d %H:%M:%S', errors='coerce', infer_datetime_format=True)
            df[config[parameter]['datetime']] = df[config[parameter]['datetime']].dt.strftime('%m/%d/%Y')
            upload(df)

        if parameter == "conductivity":
            df = sql_time()
            df = auto_timestamp_column()
            df = est_column()
            df = lock_column()
            df = site_id(site_sql_id)
            df = utc_offset_column(utc_offset)
            df = warning_column()
            # ONLY USE THIS FOR SQL IMPORT IT ADDS & HOURS
            
            upload(df)

        if parameter == "discharge":
            # print(df)
            df = auto_timestamp_column()
            df = discharge_column()
            df = est_column()
            df = lock_column()
            df = provisional_column()
            df = utc_offset_column(utc_offset)
            df = warning_column()
            df = provisional_column()
            df = site_id(site_sql_id)
            # ONLY USE THIS FOR SQL IMPORT IT ADDS & HOURS
            df = sql_time()
            df.drop(columns=['stage', 'water_level'], inplace=True)
            upload(df)

        if parameter == "dissolved_oxygen":
            # first put into sql time for querying additional values
            # ONLY USE THIS FOR SQL IMPORT IT ADDS & HOURS
            df = sql_time()
            # get water temperature from record before calculating additional values
            df = water_temperature_record_column()
            # renames water temperature column for dissolved oxygen tables
            df = dissolved_oxygen_water_temperature_column()
            df = dissolved_oxygen_precent_column()
            auto_timestamp_column()
            df = est_column()
            df = lock_column()
            df = site_id(site_sql_id)
            df = utc_offset_column(utc_offset)
            df = warning_column()
            df = provisional_column()
            upload(df)

        if parameter == "Piezometer":
            df = auto_timestamp_column()
            df = est_column()
            df = gallons_pumped_column()
            df = lock_column()
            df = provisional_column()
            df = pump_on_column()
            df = site_id(site_sql_id)
            df = utc_offset_column(utc_offset)
            df = warning_column()
            # ONLY USE THIS FOR SQL IMPORT IT ADDS & HOURS
            df = sql_time()

        if parameter == "rain":
            df = auto_timestamp_column()
            df = est_column()
            df = lock_column()
            df = provisional_column()
            df = site_id(site_sql_id)
            df = snow_column()
            df = utc_offset_column(utc_offset)
            df = warning_column()
            # ONLY USE THIS FOR SQL IMPORT IT ADDS & HOURS
            df = sql_time()
            upload(df)

        if parameter == "rain_tips":
            df = auto_timestamp_column()
            df = est_column()
            df = lock_column()
            df = provisional_column()
            df = site_id(site_sql_id)
            df = snow_column()
            df = utc_offset_column(utc_offset)
            df = warning_column()
            # ONLY USE THIS FOR SQL IMPORT IT ADDS & HOURS
            df = sql_time()
            upload(df)

        if parameter == "turbidity":
            df = auto_timestamp_column()
            df = est_column()
            df = lock_column()
            df = provisional_column()
            df = site_id(site_sql_id)
            df = utc_offset_column(utc_offset)
            df = warning_column()
            # ONLY USE THIS FOR SQL IMPORT IT ADDS & HOURS
            df = sql_time()
            upload(df)

        if parameter == "water_level":
            df = auto_timestamp_column()
            df = est_column()
            df = lock_column()
            df = provisional_column()
            df = site_id(site_sql_id)
            df = utc_offset_column(utc_offset)
            df = warning_column()
            # ONLY USE THIS FOR SQL IMPORT IT ADDS & HOURS
            df = sql_time()
            upload(df)

        if parameter == "water_temperature":
            df = auto_timestamp_column()
            df = est_column()
            df = ice_column()
            df = depth_column()
            df = provisional_column()
            df = site_id(site_sql_id)
            df = utc_offset_column(utc_offset)
            # ONLY USE THIS FOR SQL IMPORT IT ADDS & HOURS
            df = sql_time()
            upload(df)

        return df

    def daily_table(parameter, site_sql_id):
        '''updates daily table, regardless of weither data was uploaded
        behaves similarly to discharge upload function'''
        # get 15 minute data last value
        # agnostic to actual interval
        try:
            existing_data = cursor.execute("select max("+str(config[parameter]['datetime'])+") from "+str(config[parameter]['table'])+" WHERE G_ID = "+str(site_sql_id)+";").fetchval().date()
        except:
                # havent verified this but if there is no existing data the query throws an error
                existing_data = datetime.strptime("1900-1-1", '%Y-%m-%d').date()
        # get daily table last value
        try:
            existing_daily_data = cursor.execute("select max("+str(config[parameter]['daily_datetime'])+") from "+str(config[parameter]['daily_table'])+" WHERE G_ID = "+str(site_sql_id)+";").fetchval().date()
        except AttributeError:
            # if there is no data present
            existing_daily_data = datetime.strptime("1900-1-1", '%Y-%m-%d').date()
        # def discharge_column():
            # df.rename(columns={"discharge": config[parameter]['discharge']}, inplace=True)
            # return df

        def est_column():
            data[config[parameter]["daily_estimate"]] = "0"
            return data

        def depth_column():
            data[config[parameter]["daily_depth"]] = "0"
            return data

        def ice_column():
            data[config[parameter]["daily_ice"]] = "0"
            return data

        def lock_column():
            data[config[parameter]["daily_lock"]] = "0"
            return data

        def warning_column():
            data[config[parameter]["daily_warning"]] = "0"
            return data

        def provisional_column():
            data[config[parameter]['daily_provisional']] = "0"
            return data

        def gallons_pumped_column():
            data[config[parameter]['gallons_pumped']] = ""
            return data

        def pump_on_column():
            data[config[parameter]['pump_on']] = "0"
            return data

        def auto_timestamp_column():
            # time_now = pd.to_datetime('today')
            data[config[parameter]['daily_auto_timestamp']] = pd.to_datetime('today')
            data[config[parameter]['daily_auto_timestamp']] = data[config[parameter]['daily_auto_timestamp']].dt.strftime('%m/%d/%Y %H:%M')
            #data[config[parameter]["daily_provisional"]] = "-1"
            return data

        def utc_offset_column(utc_offset):
            data[config[parameter]['utc_offset']] = str(utc_offset)
            return data

        def snow_column():
            data[config[parameter]['daily_snow']] = "0"
            return data

        def site_id(site_sql_id):
            data["G_ID"] = str(site_sql_id)
            return data

        def sql_time(utc_offset):
            #data[config[parameter]['datetime']] = data[config[parameter]['datetime']].astype(float)
            
            data[config[parameter]['datetime']] = data[config[parameter]['datetime']].dt.strftime('%m/%d/%Y')
            data[config[parameter]["daily_datetime"]] = data[config[parameter]['datetime']]
            return data

         # The actual daily data upload
        def daily_upload(data):
            sql_alchemy_connection = urllib.parse.quote_plus('DRIVER={'+driver+'}; SERVER='+server+'; DATABASE='+database+'; Trusted_Connection='+trusted_connection+';')
            sql_engine = create_engine("mssql+pyodbc:///?odbc_connect=%s" % sql_alchemy_connection)
            cnxn = sql_engine.raw_connection()
            data.to_sql(config[parameter]['daily_table'], sql_engine, method=None, if_exists='append', index=False)
            # try method=multi, None works
            # try chunksize int
            print("daily "+str(parameter)+" "+str(site_name)+" ["+str(site_sql_id)+"] inserted "+str(data.shape[0])+" records")
            cnxn.close()

        # if the daily table needs updating
        if existing_daily_data < existing_data:
            end_date = existing_data
            # pull old data + 1 day
            start_date = existing_daily_data - timedelta(days=2)
            # new_data = pd.read_sql_query('select '+config[parameter]['datetime']+','+config[parameter]['corrected_data']+','+config[parameter]['discharge']+' from '+config[parameter]['table']+' WHERE G_ID = '+str(site_sql_id)+' AND '+config[parameter]['datetime']+' between ? and ?', conn, params=[str(start_date), str(end_date)])
            # Delete existing data for time peroid in question
            conn.execute(f"delete from {config[parameter]['daily_table']} WHERE G_ID = {site_sql_id} AND {config[parameter]['daily_datetime']} between ? and ?", start_date.strftime('%m/%d/%Y'), end_date.strftime('%m/%d/%Y'))
            conn.commit()

            if parameter == "discharge":
                new_data = pd.read_sql_query('select '+config[parameter]['datetime']+','+config[parameter]['corrected_data']+','+config[parameter]['discharge']+' from '+config[parameter]['table']+' WHERE G_ID = '+str(site_sql_id)+' AND '+config[parameter]['datetime']+' between ? and ?', conn, params=[str(start_date), str(end_date)])
                new_data.rename(columns={
                    config[parameter]['datetime']: "datetime",
                    config[parameter]['corrected_data']: "corrected_data",
                    config[parameter]['discharge']: "discharge",
                }, inplace=True)

            else:
                new_data = pd.read_sql_query('select '+config[parameter]['datetime']+','+config[parameter]['corrected_data']+' from '+config[parameter]['table']+' WHERE G_ID = '+str(site_sql_id)+' AND '+config[parameter]['datetime']+' between ? and ?', conn, params=[str(start_date), str(end_date)])
                new_data.rename(columns={
                    config[parameter]['datetime']: "datetime",
                    config[parameter]['corrected_data']: "corrected_data",
                }, inplace=True)
            if parameter == "rain":
                # resample 15 minute to daily
                new_data.set_index('datetime', inplace=True)
                corrected_data = new_data.resample('D')['corrected_data'].agg(['sum', 'count'])
                corrected_data.reset_index(inplace=True)

                # corrected_data = corrected_data[["datetime":config[parameter]['datetime'], "mean":config[parameter]['corrected_data_mean'], "max":config[parameter]['corrected_data_max'], "min":config[parameter]['D_MinStage'], "count":config[parameter]['daily_record_count']]].copy
                corrected_data.rename(columns={
                    "datetime": config[parameter]["datetime"],
                    "sum": config[parameter]["daily_sum"],
                    "count": config[parameter]["daily_record_count"],
                }, inplace=True)

            else:
                # resample 15 minute to daily
                new_data.set_index('datetime', inplace=True)
                corrected_data = new_data.resample('D')['corrected_data'].agg(['mean', 'max', 'min', 'count'])
                corrected_data.reset_index(inplace=True)
                # corrected_data = corrected_data[["datetime":config[parameter]['datetime'], "mean":config[parameter]['corrected_data_mean'], "max":config[parameter]['corrected_data_max'], "min":config[parameter]['D_MinStage'], "count":config[parameter]['daily_record_count']]].copy
                corrected_data.rename(columns={
                    "datetime": config[parameter]["datetime"],
                    "mean": config[parameter]["daily_mean"],
                    "max": config[parameter]["daily_max"],
                    "min": config[parameter]["daily_min"],
                    "count": config[parameter]["daily_record_count"],
                }, inplace=True)

            if parameter == "air_temperature":
                data = corrected_data
                # add other columns
                data = auto_timestamp_column()
                # df = discharge_column()
                data = est_column()
                data = lock_column()
                data = provisional_column()
                data = utc_offset_column(utc_offset)
                data = warning_column()
                data = site_id(site_sql_id)
                # ONLY USE THIS FOR SQL IMPORT IT ADDS & HOURS
                data = sql_time(utc_offset)
                # drop columns
                data.drop(columns=[config[parameter]["datetime"], config[parameter]["utc_offset"]], inplace=True)
                daily_upload(data)

            if parameter == "water_temperature":
                data = corrected_data
                # add other columns
                data = auto_timestamp_column()
                # df = discharge_column()
                data = est_column()
                data = depth_column()
                data = ice_column()
                data = lock_column()
                data = provisional_column()
                data = utc_offset_column(utc_offset)
                data = warning_column()
                data = site_id(site_sql_id)
                # ONLY USE THIS FOR SQL IMPORT IT ADDS & HOURS
                data = sql_time(utc_offset)
                # drop columns
                data.drop(columns=[config[parameter]["datetime"], config[parameter]["utc_offset"]], inplace=True)
                daily_upload(data)

            if parameter == "barometer":
                data = corrected_data
            # add other columns
                data = auto_timestamp_column()
                # df = discharge_column()
                data = est_column()
                data = lock_column()
                data = provisional_column()
                data = utc_offset_column(utc_offset)
                data = warning_column()
                data = site_id(site_sql_id)
                # ONLY USE THIS FOR SQL IMPORT IT ADDS & HOURS
                data = sql_time(utc_offset)
                # drop columns
                data.drop(columns=[config[parameter]["datetime"], config[parameter]["utc_offset"]], inplace=True)
                daily_upload(data)

            if parameter == "conductivity":
                data = corrected_data
                # add other columns
                data = auto_timestamp_column()
                # df = discharge_column()
                data = est_column()
                data = lock_column()
                data = utc_offset_column(utc_offset)
                data = warning_column()
                data = site_id(site_sql_id)
                # ONLY USE THIS FOR SQL IMPORT IT ADDS & HOURS
                data = sql_time(utc_offset)
                # drop columns
                data.drop(columns=[config[parameter]["datetime"], config[parameter]["utc_offset"]], inplace=True)
                daily_upload(data)

            if parameter == "discharge":
                discharge = new_data.resample('D')['discharge'].agg(['mean', 'max', 'min'])
                discharge.reset_index(inplace=True)
                discharge.rename(columns={
                    "datetime": config[parameter]["datetime"],
                    "mean": config[parameter]["discharge_mean"],
                    "max": config[parameter]["discharge_max"],
                    "min": config[parameter]["discharge_min"],
                }, inplace=True)
                data = corrected_data.merge(discharge, left_on=config[parameter]["datetime"], right_on=config[parameter]["datetime"])
            # add other columns
                data = auto_timestamp_column()
                # df = discharge_column()
                data = est_column()
                data = lock_column()
                data = provisional_column()
                data = utc_offset_column(utc_offset)
                data = warning_column()
                data = site_id(site_sql_id)
                # ONLY USE THIS FOR SQL IMPORT IT ADDS & HOURS
                data = sql_time(utc_offset)
                # drop columns
                data.drop(columns=[config[parameter]["datetime"], config[parameter]["utc_offset"]], inplace=True)
                daily_upload(data)

            if parameter == "water_level":
                data = corrected_data
            # add other columns
                data = auto_timestamp_column()
                # df = discharge_column()
                data = est_column()
                data = lock_column()
                data = provisional_column()
                data = utc_offset_column(utc_offset)
                data = warning_column()
                data = site_id(site_sql_id)
                # ONLY USE THIS FOR SQL IMPORT IT ADDS & HOURS
                data = sql_time(utc_offset)
                # drop columns
                data.drop(columns=[config[parameter]["datetime"], config[parameter]["utc_offset"]], inplace=True)
                daily_upload(data)
                
            if parameter == "rain":
                data = corrected_data
            # add other columns
                data = auto_timestamp_column()
                # df = discharge_column()
                data = est_column()
                data = snow_column()
                data = lock_column()
                data = provisional_column()
                data = utc_offset_column(utc_offset)
                data = warning_column()
                data = site_id(site_sql_id)
                # ONLY USE THIS FOR SQL IMPORT IT ADDS & HOURS
                data = sql_time(utc_offset)
                # drop columns
                data.drop(columns=[config[parameter]["datetime"], config[parameter]["utc_offset"]], inplace=True)
                daily_upload(data)

            if parameter == "turbidity":
                data = corrected_data
            # add other columns
                data = auto_timestamp_column()
                # df = discharge_column()
                data = est_column()
                data = lock_column()
                data = provisional_column()
                data = utc_offset_column(utc_offset)
                data = warning_column()
                data = site_id(site_sql_id)
                # ONLY USE THIS FOR SQL IMPORT IT ADDS & HOURS
                data = sql_time(utc_offset)
                # drop columns
                data.drop(columns=[config[parameter]["datetime"], config[parameter]["utc_offset"]], inplace=True)
                daily_upload(data)
        # if the daily table does not need updating
        else:
            # return an empty data frame, a bit hacky but it prevents needless blank sql inserts later
            data = []
            data = pd.DataFrame(data, columns=[])


    # call water temperature
    print("run water temperature")
    # Define what we are looking for
    parameter = "water_temperature"
    file_path = r"W:\STS\hydro\GAUGE\zTelemetered_Tables\Cdrive_table15_copied"
    telemetry_table = "tblWaterTempGageRun"
    # Querty Approperate telemetry table (reference table)
    reference_table = get_reference_table(parameter, file_path, telemetry_table)
    for index, row in reference_table.iterrows():
        # Lookup different variables in reference telemetry table
        site_sql_id = reference_table.iloc[index, 0]
        site_name = get_site_name(reference_table)
        file_name = reference_table.iloc[index, 1]
        column_number = reference_table.iloc[index, 2]
        utc_offset = reference_table.iloc[index, 3]
        number_of_header_rows = reference_table.iloc[index, 4]
        # if there is an offset read this row
        # othersiwe there wont be a 5th row
        try:
            offset = reference_table.iloc[index, 5]
        except Exception:
            offset = 0
        parameter_upload_data = open_file(parameter, site_name, site_sql_id, file_path, file_name)
        if parameter_upload_data.empty:
            continue
        clean_file(
            parameter_upload_data,
            offset, parameter,
            site_name,
            site_sql_id,
            utc_offset)
        cursor = conn.cursor()
        df = cut_data(parameter, parameter_upload_data, site_sql_id)
        if not df.empty:
            upload_data(df, parameter, site_sql_id, utc_offset)
        daily_table(parameter, site_sql_id)
        # Time_Check(parameter_table, File_Path, Telemetry_Table, File_Name, CHECK_CSV, utc_offset, sql_prefix)
        cursor.close()
    print("water temperature complete")

    # Discharge was built with a less advanced layout, I left it this way for now as it doesnt easly fit into the above function
    # call Discharge Table
    print("Run Discharge")
    reference_table = pd.read_sql_query('select G_ID, File_Name, Column_Number, UTC_Offset, NumberOfHeaderRows, Instrument_Offset, Flow_Rating_ID, Rating_Offset from tblFlowGageRun', conn)
    # For every row in the Telemetry table
    for index, row in reference_table.iterrows():
        # Define what we are looking for
        parameter = "discharge"
        file_path = r"W:\STS\hydro\GAUGE\zTelemetered_Tables\Cdrive_table15_copied"
        telemetry_table = "tblFlowGageRun"
        # Lookup different variables in reference telemetry table
        site_sql_id = str(reference_table.iloc[index, 0])
        site_name = get_site_name(reference_table)
        file_name = reference_table.iloc[index, 1]
        column_number = reference_table.iloc[index, 2]
        utc_offset = reference_table.iloc[index, 3]
        number_of_header_rows = reference_table.iloc[index, 4]
        instrument_offset = reference_table.iloc[index, 5]
        offset = instrument_offset
        Rating = reference_table.iloc[index, 6]
        rating_offset = reference_table.iloc[index, 7]
        try:
            parameter_upload_data = pd.read_csv(r'W:\STS\hydro\GAUGE\zTelemetered_Tables\Cdrive_table15_copied'+str("\\")+str(file_name), header=number_of_header_rows, usecols=[0, column_number])
        except FileNotFoundError:
            print(str(parameter)+" "+str(site_name)+" ["+str(site_sql_id)+"] File is not present")
            continue
        except OSError:
            print(str(parameter)+" "+str(site_name)+" ["+str(site_sql_id)+"] OS Error, invalid file path")
            continue
        except Exception:
            try:
                parameter_upload_data = pd.read_csv(r'W:\STS\hydro\GAUGE\zTelemetered_Tables\Cdrive_table15_copied'+str('\\')+str(file_name), header=number_of_header_rows, usecols=[0, column_number], encoding="ISO-8859-1")
            except Exception:
                print(str(parameter)+" "+str(site_name)+" ["+str(site_sql_id)+"] Unknown File Load Error")
                continue
        parameter_upload_data.rename(columns={
            parameter_upload_data.columns[0]: "datetime",
            }, inplace=True)
        clean_file(
            parameter_upload_data,
            offset, parameter,
            site_name,
            site_sql_id,
            utc_offset)
        parameter_upload_data['stage'] = parameter_upload_data['corrected_data']
        # Calculate DIscharge
        # get rating number from Rating ID via tblFlowRating_Stats
        rating_number = pd.read_sql_query(f"select Rating_Number from tblFlowRating_Stats WHERE FLowRating_ID = {Rating}", conn)
        rating_number["Rating_Number"] = rating_number["Rating_Number"].str.rstrip()
        rating_number = rating_number.iloc[0, 0]
        # get initial rating offset from tblFlowRating_Stats
        gzf = pd.read_sql_query(f"select Offset from tblFlowRating_Stats WHERE FLowRating_ID = {Rating}", conn)
        gzf = gzf.iloc[0, 0]
        # Subtract GZF from Stage
        parameter_upload_data['water_level'] = parameter_upload_data['stage']-gzf
        # Get rid of bad values
        pd.options.mode.use_inf_as_na = True
        parameter_upload_data.replace("", "NA", inplace=True)
        parameter_upload_data.replace('"NAN"', "NA", inplace=True)
        parameter_upload_data.dropna(inplace=True)
        # Incoperate Rating Offset This is taken from the telemetry table
        parameter_upload_data['water_level'] = parameter_upload_data['water_level']+rating_offset
        # get wl/Q for  matching stage to rating with tblFLowRatings
        ratings = pd.read_sql_query(f"select RatingNumber, WaterLevel, Discharge from tblFlowRatings WHERE G_ID = {site_sql_id};", conn)

        ratings.rename(columns={
            ratings.columns[0]: "rating_number",
            ratings.columns[1]: "water_level",
            ratings.columns[2]: "discharge"
            }, inplace=True)
        ratings['rating_number'] = ratings['rating_number']
        ratings_selection = ratings[ratings['rating_number'] == str(rating_number)]
        ratings_selection = ratings_selection[["water_level", "discharge"]]
        # Match stage to discharge rating curve
        parameter_upload_data = pd.merge_asof(parameter_upload_data.sort_values('water_level'), ratings_selection.sort_values('water_level'), on='water_level', allow_exact_matches=False, direction='nearest')
        parameter_upload_data = parameter_upload_data.sort_values(by="datetime")
        cursor = conn.cursor()
        df = cut_data(parameter, parameter_upload_data, site_sql_id)
        if not df.empty:
            upload_data(df, parameter, site_sql_id, utc_offset)
        daily_table(parameter, site_sql_id)
        # Time_Check(parameter_table, File_Path, Telemetry_Table, File_Name, CHECK_CSV, utc_offset, sql_prefix)
        cursor.close()
    print("Discharge Complete")

    ### beginning of more standard nomenclature ###
    # call waterlevel
    print("run water level")
    # Define what we are looking for
    parameter = "water_level"
    file_path = r"W:\STS\hydro\GAUGE\zTelemetered_Tables\Cdrive_table15_copied"
    telemetry_table = "tblLakeLevelGageRun"
    # Querty Approperate telemetry table (reference table)
    reference_table = get_reference_table(parameter, file_path, telemetry_table)
    for index, row in reference_table.iterrows():
        # Lookup different variables in reference telemetry table
        site_sql_id = reference_table.iloc[index, 0]
        site_name = get_site_name(reference_table)
        file_name = reference_table.iloc[index, 1]
        column_number = reference_table.iloc[index, 2]
        utc_offset = reference_table.iloc[index, 3]
        number_of_header_rows = reference_table.iloc[index, 4]
        # if there is an offset read this row
        # othersiwe there wont be a 5th row
        try:
            offset = reference_table.iloc[index, 5]
        except Exception:
            offset = 0
        parameter_upload_data = open_file(parameter, site_name, site_sql_id, file_path, file_name)
        if parameter_upload_data.empty:
            continue
        clean_file(
            parameter_upload_data,
            offset, parameter,
            site_name,
            site_sql_id,
            utc_offset)
        cursor = conn.cursor()
        df = cut_data(parameter, parameter_upload_data, site_sql_id)
        if not df.empty:
            upload_data(df, parameter, site_sql_id, utc_offset)
        daily_table(parameter, site_sql_id)
        # Time_Check(parameter_table, File_Path, Telemetry_Table, File_Name, CHECK_CSV, utc_offset, sql_prefix)
        cursor.close()
    print("water level complete")

    # call conductivity
    print("run conductivity")
    # Define what we are looking for
    parameter = "conductivity"
    file_path = r"W:\STS\hydro\GAUGE\zTelemetered_Tables\Cdrive_table15_copied"
    telemetry_table = "tblConductivityGageRun"
    # Querty Approperate telemetry table (reference table)
    reference_table = get_reference_table(parameter, file_path, telemetry_table)
    for index, row in reference_table.iterrows():
        # Lookup different variables in reference telemetry table
        site_sql_id = reference_table.iloc[index, 0]
        site_name = get_site_name(reference_table)
        file_name = reference_table.iloc[index, 1]
        column_number = reference_table.iloc[index, 2]
        utc_offset = reference_table.iloc[index, 3]
        number_of_header_rows = reference_table.iloc[index, 4]
        # if there is an offset read this row
        # othersiwe there wont be a 5th row
        try:
            offset = reference_table.iloc[index, 5]
        except Exception:
            offset = 0
        parameter_upload_data = open_file(parameter, site_name, site_sql_id, file_path, file_name)
        if parameter_upload_data.empty:
            continue
        clean_file(
            parameter_upload_data,
            offset, parameter,
            site_name,
            site_sql_id,
            utc_offset)
        cursor = conn.cursor()
        df = cut_data(parameter, parameter_upload_data, site_sql_id)
        if not df.empty:
            upload_data(df, parameter, site_sql_id, utc_offset)
        daily_table(parameter, site_sql_id)
        # Time_Check(parameter_table, File_Path, Telemetry_Table, File_Name, CHECK_CSV, utc_offset, sql_prefix)
        cursor.close()
    print("water level complete")
 
    # call dissolved oxygen
    print("run dissolved oxygen")
    # Define what we are looking for
    parameter = "dissolved_oxygen"
    file_path = r"W:\STS\hydro\GAUGE\zTelemetered_Tables\Cdrive_table15_copied"
    telemetry_table = "tblDOGageRun"
    # Querty Approperate telemetry table (reference table)
    reference_table = get_reference_table(parameter, file_path, telemetry_table)
    for index, row in reference_table.iterrows():
        # Lookup different variables in reference telemetry table
        site_sql_id = reference_table.iloc[index, 0]
        site_name = get_site_name(reference_table)
        file_name = reference_table.iloc[index, 1]
        column_number = reference_table.iloc[index, 2]
        utc_offset = reference_table.iloc[index, 3]
        number_of_header_rows = reference_table.iloc[index, 4]
        # if there is an offset read this row
        # othersiwe there wont be a 5th row
        try:
            offset = reference_table.iloc[index, 5]
        except Exception:
            offset = 0
        parameter_upload_data = open_file(parameter, site_name, site_sql_id, file_path, file_name)
        if parameter_upload_data.empty:
            continue
        clean_file(
            parameter_upload_data,
            offset, parameter,
            site_name,
            site_sql_id,
            utc_offset)
        cursor = conn.cursor()
        df = cut_data(parameter, parameter_upload_data, site_sql_id)
        if not df.empty:
            upload_data(df, parameter, site_sql_id, utc_offset)
        daily_table(parameter, site_sql_id)
        # Time_Check(parameter_table, File_Path, Telemetry_Table, File_Name, CHECK_CSV, utc_offset, sql_prefix)
        cursor.close()
    print("dissolved oxygen")

    # call turbidity
    print("run turbidity")
    # Define what we are looking for
    parameter = "turbidity"
    file_path = r"W:\STS\hydro\GAUGE\zTelemetered_Tables\Cdrive_table15_copied"
    telemetry_table = "tblTurbidityGageRun"
    # Querty Approperate telemetry table (reference table)
    reference_table = get_reference_table(parameter, file_path, telemetry_table)
    for index, row in reference_table.iterrows():
        # Lookup different variables in reference telemetry table
        site_sql_id = reference_table.iloc[index, 0]
        site_name = get_site_name(reference_table)
        file_name = reference_table.iloc[index, 1]
        column_number = reference_table.iloc[index, 2]
        utc_offset = reference_table.iloc[index, 3]
        number_of_header_rows = reference_table.iloc[index, 4]
        # if there is an offset read this row
        # othersiwe there wont be a 5th row
        try:
            offset = reference_table.iloc[index, 5]
        except Exception:
            offset = 0
        parameter_upload_data = open_file(parameter, site_name, site_sql_id, file_path, file_name)
        if parameter_upload_data.empty:
            continue
        clean_file(
            parameter_upload_data,
            offset, parameter,
            site_name,
            site_sql_id,
            utc_offset)
        cursor = conn.cursor()
        df = cut_data(parameter, parameter_upload_data, site_sql_id)
        if not df.empty:
            upload_data(df, parameter, site_sql_id, utc_offset)
        daily_table(parameter, site_sql_id)
        # Time_Check(parameter_table, File_Path, Telemetry_Table, File_Name, CHECK_CSV, utc_offset, sql_prefix)
        cursor.close()
    print("turbidity complete")

    # call air temperature
    print("run air temperature")
    # Define what we are looking for
    parameter = "air_temperature"
    file_path = r"W:\STS\hydro\GAUGE\zTelemetered_Tables\Cdrive_table15_copied"
    telemetry_table = "tblAirTemperatureGageRun"
    # Querty Approperate telemetry table (reference table)
    reference_table = get_reference_table(parameter, file_path, telemetry_table)
    for index, row in reference_table.iterrows():
        # Lookup different variables in reference telemetry table
        site_sql_id = reference_table.iloc[index, 0]
        site_name = get_site_name(reference_table)
        file_name = reference_table.iloc[index, 1]
        column_number = reference_table.iloc[index, 2]
        utc_offset = reference_table.iloc[index, 3]
        number_of_header_rows = reference_table.iloc[index, 4]
        # if there is an offset read this row
        # othersiwe there wont be a 5th row
        try:
            offset = reference_table.iloc[index, 5]
        except Exception:
            offset = 0
        parameter_upload_data = open_file(parameter, site_name, site_sql_id, file_path, file_name)
        if parameter_upload_data.empty:
            continue
        clean_file(
            parameter_upload_data,
            offset, parameter,
            site_name,
            site_sql_id,
            utc_offset)
        cursor = conn.cursor()
        df = cut_data(parameter, parameter_upload_data, site_sql_id)
        if not df.empty:
            upload_data(df, parameter, site_sql_id, utc_offset)
        daily_table(parameter, site_sql_id)
        # Time_Check(parameter_table, File_Path, Telemetry_Table, File_Name, CHECK_CSV, utc_offset, sql_prefix)
        cursor.close()
    print("air temperature complete")

    # call barometer
    print("run barometer")
    # Define what we are looking for
    parameter = "barometer"
    file_path = r"W:\STS\hydro\GAUGE\zTelemetered_Tables\Cdrive_table15_copied"
    telemetry_table = "tblBarometerGageRun"
    # Querty Approperate telemetry table (reference table)
    reference_table = get_reference_table(parameter, file_path, telemetry_table)
    for index, row in reference_table.iterrows():
        # Lookup different variables in reference telemetry table
        site_sql_id = reference_table.iloc[index, 0]
        site_name = get_site_name(reference_table)
        file_name = reference_table.iloc[index, 1]
        column_number = reference_table.iloc[index, 2]
        utc_offset = reference_table.iloc[index, 3]
        number_of_header_rows = reference_table.iloc[index, 4]
        # if there is an offset read this row
        # othersiwe there wont be a 5th row
        try:
            offset = reference_table.iloc[index, 5]
        except Exception:
            offset = 0
        parameter_upload_data = open_file(parameter, site_name, site_sql_id, file_path, file_name)
        if parameter_upload_data.empty:
            continue
        clean_file(
            parameter_upload_data,
            offset, parameter,
            site_name,
            site_sql_id,
            utc_offset)
        cursor = conn.cursor()
        df = cut_data(parameter, parameter_upload_data, site_sql_id)
        if not df.empty:
            upload_data(df, parameter, site_sql_id, utc_offset)
        daily_table(parameter, site_sql_id)
        # Time_Check(parameter_table, File_Path, Telemetry_Table, File_Name, CHECK_CSV, utc_offset, sql_prefix)
        cursor.close()
    print("barometer complete")
    
    # call rain
    print("run rain")
    # Define what we are looking for
    parameter = "rain"
    #print(config[parameter]['datetime'])
    file_path = r"W:\STS\hydro\GAUGE\zTelemetered_Tables\Cdrive_table15_copied"
    telemetry_table = "tblRainGageRun"
    # Querty Approperate telemetry table (reference table)
    reference_table = get_reference_table(parameter, file_path, telemetry_table)
    for index, row in reference_table.iterrows():
        # Lookup different variables in reference telemetry table
        site_sql_id = reference_table.iloc[index, 0]
        site_name = get_site_name(reference_table)
        file_name = reference_table.iloc[index, 1]
        column_number = reference_table.iloc[index, 2]
        utc_offset = reference_table.iloc[index, 3]
        number_of_header_rows = reference_table.iloc[index, 4]
        # if there is an offset read this row
        # othersiwe there wont be a 5th row
        try:
            offset = reference_table.iloc[index, 5]
        except Exception:
            offset = 0
        parameter_upload_data = open_file(parameter, site_name, site_sql_id, file_path, file_name)
        if parameter_upload_data.empty:
            continue
        clean_file(
            parameter_upload_data,
            offset, parameter,
            site_name,
            site_sql_id,
            utc_offset)
        cursor = conn.cursor()
        df = cut_data(parameter, parameter_upload_data, site_sql_id)
        if not df.empty:
            upload_data(df, parameter, site_sql_id, utc_offset)
        daily_table(parameter, site_sql_id)
        # Time_Check(parameter_table, File_Path, Telemetry_Table, File_Name, CHECK_CSV, utc_offset, sql_prefix)
        cursor.close()
    print("rain complete")

    # call rain_tips
    print("run rain tips")
    # Define what we are looking for
    parameter = "rain_tips"
    file_path = r"W:\STS\hydro\GAUGE\zTelemetered_Tables\Cdrive_table_tips_copied"
    telemetry_table = "tblRainGageRun"
    # Querty Approperate telemetry table (reference table)
    reference_table = get_reference_table(parameter, file_path, telemetry_table)
    for index, row in reference_table.iterrows():
        # Lookup different variables in reference telemetry table
        site_sql_id = reference_table.iloc[index, 0]
        site_name = get_site_name(reference_table)
        file_name = reference_table.iloc[index, 1]
        column_number = reference_table.iloc[index, 2]
        utc_offset = reference_table.iloc[index, 3]
        number_of_header_rows = reference_table.iloc[index, 4]
        # if there is an offset read this row
        # othersiwe there wont be a 5th row
        try:
            offset = reference_table.iloc[index, 5]
        except Exception:
            offset = 0
        parameter_upload_data = open_file(parameter, site_name, site_sql_id, file_path, file_name)
        if parameter_upload_data.empty:
            continue
        clean_file(
            parameter_upload_data,
            offset, parameter,
            site_name,
            site_sql_id,
            utc_offset)
        cursor = conn.cursor()
        df = cut_data(parameter, parameter_upload_data, site_sql_id)
        if not df.empty:
            upload_data(df, parameter, site_sql_id, utc_offset)
        # no daily table for rain tips
        # daily_table(parameter, site_sql_id)
        # Time_Check(parameter_table, File_Path, Telemetry_Table, File_Name, CHECK_CSV, utc_offset, sql_prefix)
        cursor.close()
    print("rain tips complete")

 # call battery
    print("run battery")
    # Define what we are looking for
    parameter = "battery"
    file_path = r"W:\STS\hydro\GAUGE\zTelemetered_Tables\Cdrive_table_daily_copied"
    telemetry_table = "tblBatteryRun"
    # Querty Approperate telemetry table (reference table)
    reference_table = get_reference_table(parameter, file_path, telemetry_table)
    for index, row in reference_table.iterrows():
        # Lookup different variables in reference telemetry table
        site_sql_id = reference_table.iloc[index, 0]
        site_name = get_site_name(reference_table)
        file_name = reference_table.iloc[index, 1]
        column_number = reference_table.iloc[index, 2]
        number_of_header_rows = reference_table.iloc[index, 3]
        # if there is an offset read this row
        # othersiwe there wont be a 5th row
        try:
            offset = reference_table.iloc[index, 5]
        except Exception:
            offset = 0
        parameter_upload_data = open_file(parameter, site_name, site_sql_id, file_path, file_name)
        if parameter_upload_data.empty:
            continue
        clean_file(
            parameter_upload_data,
            offset, parameter,
            site_name,
            site_sql_id,
            utc_offset)
        cursor = conn.cursor()
        df = cut_data(parameter, parameter_upload_data, site_sql_id)
        if not df.empty:
            upload_data(df, parameter, site_sql_id, utc_offset)
        # no daily table for rain tips
        # daily_table(parameter, site_sql_id)
        # Time_Check(parameter_table, File_Path, Telemetry_Table, File_Name, CHECK_CSV, utc_offset, sql_prefix)
        cursor.close()
    print("battery complete")
    conn.close()
    # uncheck these for output
    #sys.stdout = orig_stdout
    #f.close()

'''
# select this for timer
def job():
    run_upload()

schedule.every(10).minutes.do(job)

while True:
    schedule.run_pending()
    time.sleep(1)

# select this for straight run
'''
run_upload()
#sys.exit()

# other information
# schedule.every(0).hours.at(":00").do(job)
# schedule.every(0).hours.at(":15").do(job)
# schedule.every(0).hours.at(":30").do(job)
# schedule.every(0).hours.at(":45").do(job)
# schedule.every(0).hours.at(":40").do(job)
# schedule.every(0).hours.at(":50").do(job)

# schedule.every(1).day.at("10:30").do(job)
# schedule.every().to(30).minutes.do(job)
# schedule.every().monday.do(job)
# schedule.every().wednesday.at("13:15").do(job)
# schedule.every().minute.at(":17").do(job)





# print("Program End")
#  = BlockingScheduler()
#scheduler.add_job(Run_Upload, 'interval', minutes=30)
#scheduler.start()
