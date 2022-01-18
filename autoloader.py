# -*- coding: utf-8 -*-
"""
Created on Mon Aug 30 10:02:36 2021

@author: IHiggins
"""
import sys
import os
from datetime import datetime
from datetime import timedelta
import time
import win32com.client as win32
import schedule
import pyodbc
import pandas as pd
pd.options.mode.chained_assignment = None  # default='warn'

# import time
# import datetime
# from time import time, sleep
# import time

# thing to run
# from apscheduler.schedulers.blocking import BlockingScheduler


def Run_Upload():
    # this works but will update the file from run X at the start of run Y
    #sys.stdout = open("W:\STS\hydro\GAUGE\zTelemetered_Tables\Autoloader_Output_DEV.txt", 'w')
    print("Run Start at "+str(pd.to_datetime('today'))+"")
    print("")
    conn = pyodbc.connect('Driver={SQL Server};'
                          'Server=KCITSQLPRNRPX01;'
                          'Database=gData;'
                          'Trusted_Connection=yes;')
    Gage_Lookup = pd.read_sql_query(
        'select G_ID, SITE_CODE from tblGaugeLLID;', conn)

    # Funtion to send an email
    def E_Mail():
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

    def Get_Gauge_Name(Reference_Table):
        Reference_Table['G_ID'] = Reference_Table['G_ID'].astype('int64')
        site = Reference_Table.iloc[index, 0]
        search = Gage_Lookup.loc[Gage_Lookup['G_ID'].isin([site])]
        G_ID_Lookup = search.iloc[0, 1]
        Gauge_Name = G_ID_Lookup
        return Gauge_Name

    # Try to open file with different methods
    def Open_File(parameter, Gauge_Name, G_ID):
        try:
            parameter_upload_data = pd.read_csv(File_Path+str("\\")+str(File_Name),
                                        header=NumberofHeaderRows,
                                        usecols=[0, Column_Number])
            return parameter_upload_data
        except FileNotFoundError:
            print(str(parameter)+" "+str(Gauge_Name)+" ["+str(G_ID)+"] File is not present")
            parameter_upload_data = pd.DataFrame()
            return parameter_upload_data
        except OSError:
            print(str(parameter)+" "+str(Gauge_Name)+" ["+str(G_ID)+"] OS Error, invalid file path")
            parameter_upload_data = pd.DataFrame()
            return parameter_upload_data
        except ValueError:
            try:
                with open(File_Path+str("\\")+str(File_Name), encoding="utf8", errors='ignore') as f:
                    parameter_upload_data = pd.read_csv(f, header=NumberofHeaderRows, usecols=[0,Column_Number], engine='python', on_bad_lines='skip')
                return parameter_upload_data
            except:
                print(str(parameter)+" "+str(Gauge_Name)+" ["+str(G_ID)+"] Value Error, columns do not match file size")
                parameter_upload_data = pd.DataFrame()
                return parameter_upload_data
        except:  # will try different methods before passing
            try:
                print("ISO-8859-1")
                parameter_upload_data = pd.read_csv(File_Path+str('\\')+str(File_Name), header=NumberofHeaderRows, usecols=[0, Column_Number], encoding="ISO-8859-1")
                return parameter_upload_data
            except:
                try:
                    parameter_upload_data = pd.read_csv(File_Path+str('\\')+str(File_Name), header=NumberofHeaderRows, usecols=[0, Column_Number], encoding="cp1252")
                    print("cp1252")
                    return parameter_upload_data
                except:
                    print(str(parameter)+" "+str(Gauge_Name)+" ["+str(G_ID)+"] Unknown File Load Error")
                    pass

    def Clean_File(parameter_upload_data, Offset, parameter, Gauge_Name, G_ID):
        Pre_Drop = parameter_upload_data.shape[1]
        #  drops values
        #  treats -inf as NA
        pd.options.mode.use_inf_as_na = True
        parameter_upload_data.replace('"NAN"', "NA", inplace=True)
        parameter_upload_data.dropna(inplace=True)
        Post_Drop = parameter_upload_data.shape[1]
        Drops = Pre_Drop-Post_Drop
        if Drops > 5:
            print(str(parameter)+" "+str(Gauge_Name)+" ["+str(G_ID)+"] Greater then 5 consecutive NAN rows droped")
        #  Rename columns - note the value column is dynamic based on what is provided by the SQL we only read the two columns so this index method is fine
        parameter_upload_data.rename(columns={parameter_upload_data.columns[0]: str(sql_prefix)+'TimeDate'}, inplace=True)
        parameter_upload_data[str(sql_prefix)+'TimeDate'] = pd.to_datetime(parameter_upload_data[str(sql_prefix)+'TimeDate'])
        parameter_upload_data.rename(columns={parameter_upload_data.columns[1]: str(sql_prefix)+'Value'}, inplace=True)
        # Round the 'value' column and convert data type
        parameter_upload_data[str(sql_prefix)+'Value'] = pd.to_numeric(parameter_upload_data[str(sql_prefix)+'Value'], errors='coerce').astype("float")
        parameter_upload_data[str(sql_prefix)+'ValueCorrected'] = (parameter_upload_data[str(sql_prefix)+'Value']+float(Offset)).round(2)
        parameter_upload_data['G_ID'] = str(G_ID)
        TIME = pd.to_datetime('today')
        parameter_upload_data['AutoDTStamp'] = TIME
        parameter_upload_data['AutoDTStamp'] = parameter_upload_data['AutoDTStamp'].dt.strftime('%m/%d/%Y %H:%M')
        return parameter_upload_data

    def Second_Trip(parameter_upload_data):
        # pd.options.mode.use_inf_as_na = True
        # parameter_upload_data.replace("", "NA", inplace=True)
        # parameter_upload_data.replace('"NAN"', "NA", inplace=True)
        # parameter_upload_data.dropna(inplace=True)
        return parameter_upload_data

    def Cut_Data(parameter_table, parameter_upload_data, sql_prefix, G_ID):
        try:
            existing_data = cursor.execute("select max("+str(sql_prefix)+"TimeDate) from "+str(parameter_table)+" WHERE G_ID = "+str(G_ID)+";").fetchval()
            parameter_upload_data[str(sql_prefix)+'TimeDate'] = pd.to_datetime(parameter_upload_data[str(sql_prefix)+'TimeDate'])
            Cut_File = parameter_upload_data[parameter_upload_data[str(sql_prefix)+'TimeDate'] > existing_data]
            parameter_upload_data = Cut_File
            parameter_upload_data[str(sql_prefix)+'TimeDate'].dt.strftime('%m/%d/%Y %H:%M')
            return parameter_upload_data
        except:
            print("Error finding old data, old data may not exist")
            pass

    def Time_Check(parameter_table, File_Path, Telemetry_Table, File_Name, CHECK_CSV, UTC_Offset, sql_prefix):
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
            print(str(parameter)+" "+str(Gauge_Name)+" ["+str(G_ID)+"] Logger reporting time is greater then current time")
            print("Last CSV Date with UTC Offset of "+str(UTC_Offset)+" : "+str(LAST_DATE_CSV_UTCOffset))
            print("Current Time UTC "+str(NOW_UTC))
            print("")
        IS_OLD_DATA = "False"
        # CHeck to see if a logger is reporting out
        if LAST_DATE_CSV_UTCOffset <= (NOW_UTC - timedelta(hours=24)):
            print("")
            print(str(parameter)+" "+str(Gauge_Name)+" ["+str(G_ID)+"] Logger last reported "+str(NOW_UTC-LAST_DATE_CSV_UTCOffset))
            # if its older data it may not have updated since the time change so we will just ignore it for now
            IS_OLD_DATA = "True"
            print("")
        # Check UTC offset
        if LAST_DATE_CSV_UTCOffset != CSV_MODIFIED_UTC and IS_OLD_DATA == "False":
            print("")
            print(str(parameter)+" "+str(Gauge_Name)+" ["+str(G_ID)+"] Check UTC time on logger, and autoloader settings")
            print("Last CSV Record with UTC Offset of "+str(UTC_Offset)+" = "+str(LAST_DATE_CSV_UTCOffset))
            print("Date CSV Modified in Local time "+str(CSV_MODIFIED_LOCAL)+" Time Zone "+str(TimeZone))
            print("Date CSV Modified in UTC "+str(CSV_MODIFIED_UTC)+" ")
            print("Difference "+str(DIFF))
            if str(DIFF) == "7:00:00":
                print("Logger most likely set to PDT and Loading with a UTC_Offset of 0")
            print("")

    # Discharge was built with a less advanced layout, I left it this way for now as it doesnt easly fit into the above function
    # call Discharge Table
    print("Run Discharge")
    Reference_Table = pd.read_sql_query('select G_ID, File_Name, Column_Number, UTC_Offset, NumberOfHeaderRows, Instrument_Offset, Flow_Rating_ID, Rating_Offset from tblFlowGageRun',conn)
    sql_prefix = 'D_'
    # For every row in the Telemetry table
    for index, row in Reference_Table.iterrows():
        # Define what we are looking for
        parameter = "Discharge"
        parameter_table = 'tblDischargeGauging'
        File_Path = "W:\STS\hydro\GAUGE\zTelemetered_Tables\Cdrive_table15_copied"
        Telemetry_Table = "tblFlowGageRun"
        # Lookup different variables in reference telemetry table
        G_ID = Reference_Table.iloc[index, 0]
        Gauge_Name = Get_Gauge_Name(Reference_Table)
        File_Name = Reference_Table.iloc[index, 1]
        Column_Number = Reference_Table.iloc[index, 2]
        UTC_Offset = Reference_Table.iloc[index, 3]
        NumberofHeaderRows = Reference_Table.iloc[index, 4]
        Instrument_Offset = Reference_Table.iloc[index, 5]
        Offset = Instrument_Offset
        Rating = Reference_Table.iloc[index, 6]
        Rating_Offset = Reference_Table.iloc[index, 7]
        try:
            parameter_upload_data = pd.read_csv(r'W:\STS\hydro\GAUGE\zTelemetered_Tables\Cdrive_table15_copied'+str("\\")+str(File_Name), header=NumberofHeaderRows, usecols=[0, Column_Number])
        except FileNotFoundError:
            print(str(parameter)+" "+str(Gauge_Name)+" ["+str(G_ID)+"] File is not present")
            continue
        except OSError:
            print(str(parameter)+" "+str(Gauge_Name)+" ["+str(G_ID)+"] OS Error, invalid file path")
            continue
        except:
            try:
                parameter_upload_data = pd.read_csv(r'W:\STS\hydro\GAUGE\zTelemetered_Tables\Cdrive_table15_copied'+str('\\')+str(File_Name), header=NumberofHeaderRows, usecols=[0, Column_Number], encoding="ISO-8859-1")
            except:
                print(str(parameter)+" "+str(Gauge_Name)+" ["+str(G_ID)+"] Unknown File Load Error")
                continue
        Clean_File(parameter_upload_data, Offset, parameter, Gauge_Name, G_ID)
        CHECK_CSV = parameter_upload_data.tail(1)
        parameter_upload_data[str(sql_prefix)+'Stage'] = parameter_upload_data[str(sql_prefix)+'ValueCorrected']
        # Calculate DIscharge
        # get rating number from Rating ID via tblFlowRating_Stats
        Rating_Number = pd.read_sql_query('select Rating_Number from tblFlowRating_Stats WHERE FLowRating_ID = '+str(Rating)+'',conn)
        Rating_Number["Rating_Number"] = Rating_Number["Rating_Number"].str.rstrip()
        Rating_Number = Rating_Number.iloc[0, 0]
        # get initial rating offset from tblFlowRating_Stats
        GZF = pd.read_sql_query('select Offset from tblFlowRating_Stats WHERE FLowRating_ID = '+str(Rating)+'', conn)
        GZF = GZF.iloc[0, 0]
        # Subtract GZF from Stage
        parameter_upload_data['WaterLevel'] = parameter_upload_data[str(sql_prefix)+'Stage']-GZF
        # Get rid of bad values
        pd.options.mode.use_inf_as_na = True
        parameter_upload_data.replace("", "NA", inplace=True)
        parameter_upload_data.replace('"NAN"', "NA", inplace=True)
        parameter_upload_data.dropna(inplace=True)
        # Incoperate Rating Offset This is taken from the telemetry table
        parameter_upload_data['WaterLevel'] = parameter_upload_data['WaterLevel']+Rating_Offset
        # get wl/Q for  matching stage to rating with tblFLowRatings
        Ratings = pd.read_sql_query('select RatingNumber, WaterLevel, Discharge from tblFlowRatings WHERE G_ID = '+str(G_ID)+';',conn)
        # Remove white space from Rating nUmbers
        Ratings['RatingNumber'] = Ratings['RatingNumber'].str.rstrip()
        Ratings_Selection = Ratings[Ratings['RatingNumber'] == str(Rating_Number)]
        Ratings_Selection = Ratings_Selection[["WaterLevel", "Discharge"]]
        # Match stage to discharge rating curve
        parameter_upload_data = pd.merge_asof(parameter_upload_data.sort_values('WaterLevel'), Ratings_Selection.sort_values('WaterLevel'), on = 'WaterLevel', allow_exact_matches=False, direction='nearest')
        parameter_upload_data = parameter_upload_data.sort_values(by=str(sql_prefix)+"TimeDate")
        parameter_upload_data.rename(columns={"Discharge": "D_Discharge"}, inplace=True)
        # Convert time and date
        parameter_upload_data[str(sql_prefix)+'TimeDate'] = pd.to_datetime(parameter_upload_data[str(sql_prefix)+'TimeDate'])
        parameter_upload_data.set_index(str(sql_prefix)+'TimeDate', inplace=True)
        # Add rows for sQL entry
        parameter_upload_data.reset_index(level=None, drop=False, inplace=True)
        UTC_Offset = int(UTC_Offset)
        parameter_upload_data[str(sql_prefix)+'TimeDate'] = (parameter_upload_data[str(sql_prefix)+'TimeDate'] + timedelta(hours=UTC_Offset)).dt.strftime('%m/%d/%Y %H:%M:%S.%N')
        parameter_upload_data[str(sql_prefix)+'UTCOffset'] = str(UTC_Offset)
        parameter_upload_data['G_ID'] = G_ID
        parameter_upload_data[str(sql_prefix)+'Est'] = "0"
        parameter_upload_data[str(sql_prefix)+'Lock'] = "0"
        parameter_upload_data[str(sql_prefix)+'Warning'] = "0"
        parameter_upload_data[str(sql_prefix)+'Provisional'] = "0"
        # Find Last Existing Baro Record
        cursor = conn.cursor()
        existing_data = cursor.execute("select max(D_TimeDate) from tblDischargeGauging WHERE G_ID = "+str(G_ID)+";").fetchval()
        parameter_upload_data[str(sql_prefix)+'TimeDate'] = pd.to_datetime(parameter_upload_data[str(sql_prefix)+'TimeDate'])
        # NEED TO CHANGE THIS MODIFIER
        parameter_upload_data = parameter_upload_data[parameter_upload_data.D_TimeDate > existing_data]
        parameter_upload_data[str(sql_prefix)+'TimeDate'] = parameter_upload_data[str(sql_prefix)+'TimeDate'].dt.strftime('%m/%d/%Y %H:%M')
        try:
            Size = parameter_upload_data.shape[0]
        except:
            continue
        count = 0
        skips = 0
        for index, row in parameter_upload_data.iterrows():
            # df.loc[row,column]
            try:
                cursor = conn.cursor()
                cursor.execute('INSERT INTO tblDischargeGauging (G_ID, '+sql_prefix+'TimeDate, '+sql_prefix+'UTCOffset, '+sql_prefix+'Value, '+sql_prefix+'Stage, '+sql_prefix+'Discharge, '+sql_prefix+'Est, '+sql_prefix+'Lock, '+sql_prefix+'Warning, AutoDTStamp, '+sql_prefix+'Provisional) VALUES(?,?,?,?,?,?,?,?,?,?,?)', row.G_ID, row.D_TimeDate, row.D_UTCOffset, row.D_Value, row.D_Stage, row.D_Discharge, row.D_Est, row.D_Lock, row.D_Warning, row.AutoDTStamp, row.D_Provisional)
                count = count+1
                conn.commit()
            except:
                skips = skips+1
                continue
        cursor.close()
        if count > 0:
            print(str(parameter)+" "+str(Gauge_Name)+" ["+str(G_ID)+"] UPLOADED "+str(Size))
        if skips > 0:
            print("    "+str(parameter)+" "+str(Gauge_Name)+" ["+str(G_ID)+"] Skipped Row "+str(Size))
        Time_Check(parameter_table, File_Path, Telemetry_Table, File_Name, CHECK_CSV, UTC_Offset, sql_prefix)
    print("Discharge Complete")
    print("                  ")
    print("Run Water Temperature")

    # call Water Temperature Table
    Telemetry_Table = "tblWaterTempGageRun"
    sql_prefix = 'W_'
    # this is only used for reporting
    parameter = "Water Temperature"
    parameter_table = 'tblWaterTempGauging'
    File_Path = "W:\STS\hydro\GAUGE\zTelemetered_Tables\Cdrive_table15_copied"
    # Querty Approperate Telemetry Table
    # Some sites have a offset, some dont
    # this queryies your Telemetry Table "GageRun"
    # with both the offset and without the offset row
    try:
        Reference_Table = pd.read_sql_query('select G_ID, File_Name, Column_Number, UTC_Offset, NumberOfHeaderRows, Instrument_Offset from '+str(Telemetry_Table)+'',conn)
    except:
        Reference_Table = pd.read_sql_query('select G_ID, File_Name, Column_Number, UTC_Offset, NumberOfHeaderRows from '+str(Telemetry_Table)+'', conn)
        # Reads each row in approperate telemetry table
    for index, row in Reference_Table.iterrows():
        G_ID = Reference_Table.iloc[index, 0]
        Gauge_Name= Get_Gauge_Name(Reference_Table)
        File_Name = Reference_Table.iloc[index, 1]
        Column_Number = Reference_Table.iloc[index, 2]
        UTC_Offset = Reference_Table.iloc[index, 3]
        NumberofHeaderRows = Reference_Table.iloc[index, 4]
        # if there is an offset read this row, othersiwe there wont be a 5th row
        # the reference table has its own try/except function, it was built first, but the independence may be nice
        try:
            Offset = Reference_Table.iloc[index, 5]
        except:
            Offset = 0
        #try:
        parameter_upload_data = Open_File(parameter, Gauge_Name, G_ID)
        if parameter_upload_data.empty:
            continue
        Clean_File(parameter_upload_data, Offset, parameter, Gauge_Name, G_ID)
        CHECK_CSV = parameter_upload_data.tail(1)
        #Second_Trip(parameter_upload_data)
        UTC_Offset = int(UTC_Offset)
        parameter_upload_data[str(sql_prefix)+'TimeDate'] = (parameter_upload_data[str(sql_prefix)+'TimeDate'] + timedelta(hours=UTC_Offset)).dt.strftime('%m/%d/%Y %H:%M:%S.%N')
        parameter_upload_data[str(sql_prefix)+'UTCOffset'] = str(UTC_Offset)
        parameter_upload_data[str(sql_prefix)+'Depth'] = ""
        parameter_upload_data[str(sql_prefix)+'Est'] = "0"
        parameter_upload_data[str(sql_prefix)+'Ice'] = "0"
        parameter_upload_data[str(sql_prefix)+'Lock'] = "0"
        parameter_upload_data[str(sql_prefix)+'Warning'] = "0"
        parameter_upload_data[str(sql_prefix)+'Provisional'] = "0"
        ### Find Last Existing Baro Record
        cursor = conn.cursor()
        New_Trimmed = Cut_Data(parameter_table, parameter_upload_data, sql_prefix, G_ID)
        try:
            Size = New_Trimmed.shape[0]
        except:
            continue
        # Upload to SQL
        count = 0
        skips = 0
        for index, row in New_Trimmed.iterrows():
            try:
                cursor = conn.cursor()
                cursor.execute('INSERT INTO '+str(parameter_table)+' (G_ID, '+sql_prefix+'TimeDate, '+sql_prefix+'UTCOffset, '+sql_prefix+'Value, '+sql_prefix+'ValueCorrected, '+sql_prefix+'Est, '+sql_prefix+'Ice, '+sql_prefix+'Lock, '+sql_prefix+'Warning, AutoDTStamp, '+sql_prefix+'Provisional) VALUES(?,?,?,?,?,?,?,?,?,?,?)', row.G_ID, row.W_TimeDate, row.W_UTCOffset, row.W_Value, row.W_ValueCorrected, row.W_Est, row.W_Ice, row.W_Lock, row.W_Warning, row.AutoDTStamp, row.W_Provisional)
                conn.commit()
                count = count + 1
            except:
                skips = skips+1
                continue
        cursor.close()
        if count > 0:
            print(str(parameter)+" "+str(Gauge_Name)+" ["+str(G_ID)+"] UPLOADED "+str(Size))
        if skips > 0:
            print("    "+str(parameter)+" "+str(Gauge_Name)+" ["+str(G_ID)+"] Skipped Row "+str(skips))
        Time_Check(parameter_table, File_Path, Telemetry_Table, File_Name, CHECK_CSV, UTC_Offset, sql_prefix)
    print("Water Temperature Complete")
    print("                  ")
    print("Run Turbidity")

    # call Turbidity Table
    Telemetry_Table = "tblTurbidityGageRun"
    sql_prefix = 'T_'
    # this is only used for reporting
    parameter = "Turbidity"
    parameter_table = 'tblTurbidityGauging'
    File_Path = "W:\STS\hydro\GAUGE\zTelemetered_Tables\Cdrive_table15_copied"
    # Query Approperate Telemetry Table
    # Some sites have a offset, some dont
    # this queryies your Telemetry Table "GageRun"
    # with both the offset and without the offset row
    try:
        Reference_Table = pd.read_sql_query('select G_ID, File_Name, Column_Number, UTC_Offset, NumberOfHeaderRows, Instrument_Offset from '+str(Telemetry_Table)+'', conn)
    except:
        Reference_Table = pd.read_sql_query('select G_ID, File_Name, Column_Number, UTC_Offset, NumberOfHeaderRows from '+str(Telemetry_Table)+'', conn)
    # Get information from telemetry settings table
    for index, row in Reference_Table.iterrows():
        G_ID = Reference_Table.iloc[index, 0]
        Gauge_Name= Get_Gauge_Name(Reference_Table)
        File_Name = Reference_Table.iloc[index, 1]
        Column_Number = Reference_Table.iloc[index, 2]
        UTC_Offset = Reference_Table.iloc[index, 3]
        NumberofHeaderRows = Reference_Table.iloc[index, 4]
        # if there is an offset read this row, othersiwe there wont be a 5th row
        # the reference table has its own try/except function, it was built first, but the independence may be nice
        try:
            Offset = Reference_Table.iloc[index, 5]
        except:
            Offset = 0
        parameter_upload_data = Open_File(parameter, Gauge_Name, G_ID)
        if parameter_upload_data.empty:
            continue
        Clean_File(parameter_upload_data, Offset, parameter, Gauge_Name, G_ID)
        CHECK_CSV = parameter_upload_data.tail(1)
        # Set column informaton for SQL
        UTC_Offset = int(UTC_Offset)
        parameter_upload_data[str(sql_prefix)+'TimeDate'] = (parameter_upload_data[str(sql_prefix)+'TimeDate'] + timedelta(hours=UTC_Offset)).dt.strftime('%m/%d/%Y %H:%M:%S.%N')
        parameter_upload_data[str(sql_prefix)+'UTCOffset'] = str(UTC_Offset)
        parameter_upload_data[str(sql_prefix)+'Est'] = "0"
        parameter_upload_data[str(sql_prefix)+'Lock'] = "0"
        parameter_upload_data[str(sql_prefix)+'Warning'] = "0"
        parameter_upload_data['Provisional'] = "0"
        # Find Last Existing Baro Record
        cursor = conn.cursor()
        New_Trimmed = Cut_Data(parameter_table, parameter_upload_data, sql_prefix, G_ID)
        try:
            Size = New_Trimmed.shape[0]
        except:
            continue
        # Upload to SQL
        count = 0
        skips = 0
        for index, row in New_Trimmed.iterrows():
            try:
                cursor = conn.cursor()
                cursor.execute('INSERT INTO '+str(parameter_table)+' (G_ID, '+sql_prefix+'TimeDate, '+sql_prefix+'UTCOffset, '+sql_prefix+'Value, '+sql_prefix+'ValueCorrected, '+sql_prefix+'Est, '+sql_prefix+'Lock, '+sql_prefix+'Warning, AutoDTStamp, Provisional) VALUES(?,?,?,?,?,?,?,?,?,?)', row.G_ID, row.T_TimeDate, row.T_UTCOffset, row.T_Value, row.T_ValueCorrected, row.T_Est, row.T_Lock, row.T_Warning, row.AutoDTStamp, row.Provisional)
                conn.commit()
                count = count + 1
            except:
                skips = skips + 1
                continue
        cursor.close()
        if count > 0:
            print(str(parameter)+" "+str(Gauge_Name)+" ["+str(G_ID)+"] UPLOADED "+str(Size))
        if skips > 0:
            print("    "+str(parameter)+" "+str(Gauge_Name)+" ["+str(G_ID)+"] Skipped Row "+str(skips))
        Time_Check(parameter_table, File_Path, Telemetry_Table, File_Name, CHECK_CSV, UTC_Offset, sql_prefix)
    print("Turbidity Complete")
    print("                  ")
    print("Run Air Temperature")

    # call Air Temperature Table
    Telemetry_Table = "tblAirTemperatureGageRun"
    sql_prefix = 'A_'
    # this is only used for reporting
    parameter = "Air Temperature"
    parameter_table = 'tblAirTempGauging'
    File_Path = "W:\STS\hydro\GAUGE\zTelemetered_Tables\Cdrive_table15_copied"
    # Query Approperate Telemetry Table
    # Some sites have a offset, some dont
    # this queryies your Telemetry Table "GageRun"
    # with both the offset and without the offset row
    try:
        Reference_Table = pd.read_sql_query('select G_ID, File_Name, Column_Number, UTC_Offset, NumberOfHeaderRows, Instrument_Offset from '+str(Telemetry_Table)+'', conn)
    except:
        Reference_Table = pd.read_sql_query('select G_ID, File_Name, Column_Number, UTC_Offset, NumberOfHeaderRows from '+str(Telemetry_Table)+'', conn)
    # Get variables from approperate Telemetry rable
    for index, row in Reference_Table.iterrows():
        G_ID = Reference_Table.iloc[index, 0]
        Gauge_Name= Get_Gauge_Name(Reference_Table)
        File_Name = Reference_Table.iloc[index, 1]
        Column_Number = Reference_Table.iloc[index, 2]
        UTC_Offset = Reference_Table.iloc[index, 3]
        NumberofHeaderRows = Reference_Table.iloc[index, 4]
        # if there is an offset read this row, othersiwe there wont be a 5th row
        # the reference table has its own try/except function, it was built first, but the independence may be nice
        try:
            Offset = Reference_Table.iloc[index, 5]
        except:
            Offset = 0
        parameter_upload_data = Open_File(parameter, Gauge_Name, G_ID)
        if parameter_upload_data.empty:
            continue
        Clean_File(parameter_upload_data, Offset, parameter, Gauge_Name, G_ID)
        CHECK_CSV = parameter_upload_data.tail(1)
        # set up data for SQL upload
        UTC_Offset = int(UTC_Offset)
        parameter_upload_data[str(sql_prefix)+'TimeDate'] = (parameter_upload_data[str(sql_prefix)+'TimeDate'] + timedelta(hours=UTC_Offset)).dt.strftime('%m/%d/%Y %H:%M:%S.%N')
        parameter_upload_data[str(sql_prefix)+'UTCOffset'] = str(UTC_Offset)
        parameter_upload_data[str(sql_prefix)+'Est'] = "0"
        parameter_upload_data[str(sql_prefix)+'Lock'] = "0"
        parameter_upload_data[str(sql_prefix)+'Warning'] = "0"
        parameter_upload_data[str(sql_prefix)+'Provisional'] = "0"
        # Find Last Existing Baro Record
        cursor = conn.cursor()
        New_Trimmed = Cut_Data(parameter_table, parameter_upload_data, sql_prefix, G_ID)
        try:
            Size = New_Trimmed.shape[0]
        except:
            continue
        # Upload to SQL
        count = 0
        skips = 0
        for index, row in New_Trimmed.iterrows():
            try:
                cursor = conn.cursor()
                cursor.execute('INSERT INTO '+str(parameter_table)+' (G_ID, '+sql_prefix+'TimeDate, '+sql_prefix+'Value, '+sql_prefix+'UTCOffset, '+sql_prefix+'Est, '+sql_prefix+'Lock, '+sql_prefix+'Warning, AutoDTStamp, '+sql_prefix+'Provisional) VALUES(?,?,?,?,?,?,?,?,?)', row.G_ID, row.A_TimeDate, row.A_Value, row.A_UTCOffset, row.A_Est, row.A_Lock, row.A_Warning, row.AutoDTStamp, row.A_Provisional)
                conn.commit()
                count = count + 1
            except:
                skips = skips + 1
                continue
        cursor.close()
        if count > 0:
            print(str(parameter)+" "+str(Gauge_Name)+" ["+str(G_ID)+"] UPLOADED "+str(Size))
        if skips > 0:
            print("    "+str(parameter)+" "+str(Gauge_Name)+" ["+str(G_ID)+"] Skipped Row "+str(skips))
        Time_Check(parameter_table, File_Path, Telemetry_Table, File_Name, CHECK_CSV, UTC_Offset, sql_prefix)
    print("Air Temperature Complete")
    print("                  ")
    print("Run Water Level")

    # call Water Level Table
    Telemetry_Table = "tblLakeLevelGageRun"
    sql_prefix = 'L_'
    # this is only used for reporting
    parameter = "Water Level"
    parameter_table = 'tblLakeLevelGauging'
    File_Path = "W:\STS\hydro\GAUGE\zTelemetered_Tables\Cdrive_table15_copied"
    # Query Approperate Telemetry Table
    # Some sites have a offset, some dont
    # this queryies your Telemetry Table "GageRun"
    # with both the offset and without the offset row
    try:
        Reference_Table = pd.read_sql_query('select G_ID, File_Name, Column_Number, UTC_Offset, NumberOfHeaderRows, Instrument_Offset from '+str(Telemetry_Table)+'', conn)
    except:
        Reference_Table = pd.read_sql_query('select G_ID, File_Name, Column_Number, UTC_Offset, NumberOfHeaderRows from '+str(Telemetry_Table)+'', conn)
    for index, row in Reference_Table.iterrows():
        G_ID = Reference_Table.iloc[index, 0]
        Gauge_Name = Get_Gauge_Name(Reference_Table)
        File_Name = Reference_Table.iloc[index, 1]
        Column_Number = Reference_Table.iloc[index, 2]
        UTC_Offset = Reference_Table.iloc[index, 3]
        NumberofHeaderRows = Reference_Table.iloc[index, 4]
        # if there is an offset read this row, othersiwe there wont be a 5th row
        # the reference table has its own try/except function, it was built first, but the independence may be nice
        try:
            Offset = Reference_Table.iloc[index, 5]
        except:
            Offset = 0
        parameter_upload_data = Open_File(parameter, Gauge_Name, G_ID)
        if parameter_upload_data.empty:
            continue
        Clean_File(parameter_upload_data, Offset, parameter, Gauge_Name, G_ID)
        CHECK_CSV = parameter_upload_data.tail(1)
        # set up data for SQL upload
        UTC_Offset = int(UTC_Offset)
        parameter_upload_data[str(sql_prefix)+'TimeDate'] = (parameter_upload_data[str(sql_prefix)+'TimeDate'] + timedelta(hours=UTC_Offset)).dt.strftime('%m/%d/%Y %H:%M:%S.%N')
        parameter_upload_data[str(sql_prefix)+'UTCOffset'] = str(UTC_Offset)
        parameter_upload_data[str(sql_prefix)+'Est'] = "0"
        parameter_upload_data[str(sql_prefix)+'Lock'] = "0"
        parameter_upload_data[str(sql_prefix)+'Warning'] = "0"
        parameter_upload_data[str(sql_prefix)+'Provisional'] = "0"
        # Need to use this to change the name of the corrected table for water level etc.
        parameter_upload_data[str(sql_prefix)+'Level'] = parameter_upload_data[str(sql_prefix)+'ValueCorrected']
        # Find Last Existing Baro Record
        cursor = conn.cursor()
        New_Trimmed = Cut_Data(parameter_table, parameter_upload_data, sql_prefix, G_ID)
        try:
            Size = New_Trimmed.shape[0]
        except:
            continue
        # Upload to SQL
        count = 0
        skips = 0
        for index, row in New_Trimmed.iterrows():
            try:
                cursor = conn.cursor()
                cursor.execute('INSERT INTO '+str(parameter_table)+' (G_ID, '+sql_prefix+'TimeDate, '+sql_prefix+'UTCOffset, '+sql_prefix+'Value, '+sql_prefix+'Level, '+sql_prefix+'Est, '+sql_prefix+'Lock, '+sql_prefix+'Provisional, '+sql_prefix+'Warning, AutoDTStamp) VALUES(?,?,?,?,?,?,?,?,?,?)', row.G_ID, row.L_TimeDate, row.L_UTCOffset, row.L_Value, row.L_Level, row.L_Est, row.L_Lock, row.L_Provisional, row.L_Warning,row.AutoDTStamp)
                conn.commit()
                count = count + 1
            except:
                skips = skips + 1
                continue
        cursor.close()
        if count > 0:
            print(str(parameter)+" "+str(Gauge_Name)+" ["+str(G_ID)+"] UPLOADED "+str(Size))
        if skips > 0:
            print("    "+str(parameter)+" "+str(Gauge_Name)+" ["+str(G_ID)+"] Skipped Row "+str(skips))
        Time_Check(parameter_table, File_Path, Telemetry_Table, File_Name, CHECK_CSV, UTC_Offset, sql_prefix)
    print("Water Level Complete")
    print("                  ")
    print("Run Barometer")

    # call Barometer Table #################################################################################
    Telemetry_Table = "tblBarometerGageRun"
    sql_prefix = 'B_'
    # this is only used for reporting
    parameter = "Barometer"
    parameter_table = 'tblBarometerGauging'
    File_Path = "W:\STS\hydro\GAUGE\zTelemetered_Tables\Cdrive_table15_copied"
    # Query Approperate Telemetry Table
    # Some sites have a offset, some dont
    # this queryies your Telemetry Table "GageRun"
    # with both the offset and without the offset row
    try:
        Reference_Table = pd.read_sql_query('select G_ID, File_Name, Column_Number, UTC_Offset, NumberOfHeaderRows, Instrument_Offset from '+str(Telemetry_Table)+'', conn)
    except:
        Reference_Table = pd.read_sql_query('select G_ID, File_Name, Column_Number, UTC_Offset, NumberOfHeaderRows from '+str(Telemetry_Table)+'', conn)
    for index, row in Reference_Table.iterrows():
        # Sets values based on telemetry settings table
        G_ID = Reference_Table.iloc[index, 0]
        Gauge_Name = Get_Gauge_Name(Reference_Table)
        File_Name = Reference_Table.iloc[index, 1]
        Column_Number = Reference_Table.iloc[index, 2]
        UTC_Offset = Reference_Table.iloc[index, 3]
        NumberofHeaderRows = Reference_Table.iloc[index, 4]
        # if there is an offset read this row, othersiwe there wont be a 5th row
        # the reference table has its own try/except function, it was built first, but the independence may be nice
        try:
            Offset = Reference_Table.iloc[index, 5]
        except:
            Offset = 0
        parameter_upload_data = Open_File(parameter, Gauge_Name, G_ID)
        if parameter_upload_data.empty:
            continue
        Clean_File(parameter_upload_data, Offset, parameter, Gauge_Name, G_ID)
        CHECK_CSV = parameter_upload_data.tail(1)
        # Set up datatable for SQL upload
        UTC_Offset = int(UTC_Offset)
        parameter_upload_data[str(sql_prefix)+'TimeDate'] = (parameter_upload_data[str(sql_prefix)+'TimeDate'] + timedelta(hours=UTC_Offset)).dt.strftime('%m/%d/%Y %H:%M:%S.%N')
        parameter_upload_data[str(sql_prefix)+'UTCOffset'] = str(UTC_Offset)
        parameter_upload_data[str(sql_prefix)+'Est'] = 0
        parameter_upload_data[str(sql_prefix)+'Lock'] = 0
        parameter_upload_data[str(sql_prefix)+'Warning'] = 0
        parameter_upload_data[str(sql_prefix)+'Provisional'] = 0
        # Need to use this to change the nae of the corrected table for water level etc.
        # Find Last Existing Baro Record
        cursor = conn.cursor()
        New_Trimmed = Cut_Data(parameter_table, parameter_upload_data, sql_prefix, G_ID)
        try:
            Size = New_Trimmed.shape[0]
        except:
            continue
        # Upload to SQL
        count = 0
        skips = 0
        for index, row in New_Trimmed.iterrows():
            try:
                cursor = conn.cursor()
                cursor.execute('INSERT INTO tblBarometerGauging (G_ID, '+sql_prefix+'TimeDate, '+sql_prefix+'UTCOffset, '+sql_prefix+'Value, '+sql_prefix+'Est, '+sql_prefix+'Lock, '+sql_prefix+'Warning, AutoDTStamp, '+sql_prefix+'Provisional) VALUES(?,?,?,?,?,?,?,?,?)', row.G_ID, row.B_TimeDate, row.B_UTCOffset, row.B_Value, row.B_Est, row.B_Lock, row.B_Warning ,row.AutoDTStamp, row.B_Provisional)
                conn.commit()
                count = count + 1
            except:
                skips = skips + 1
                continue
        cursor.close()
        if count > 0:
            print(str(parameter)+" "+str(Gauge_Name)+" ["+str(G_ID)+"] UPLOADED "+str(Size))
        if skips > 0:
            print("    "+str(parameter)+" "+str(Gauge_Name)+" ["+str(G_ID)+"] Skipped Row "+str(skips))
        Time_Check(parameter_table, File_Path, Telemetry_Table, File_Name, CHECK_CSV, UTC_Offset, sql_prefix)
    print("Barometer Complete")
    print("                  ")
    print("Run Rain 15 Minutes")

    # call Rain 15 min Table
    Telemetry_Table = "tblRainGageRun"
    sql_prefix = 'R_'
    # this is only used for reporting
    parameter = "Rain 15 Minute"
    parameter_table = 'tblRainGauging'
    File_Path = "W:\STS\hydro\GAUGE\zTelemetered_Tables\Cdrive_table15_copied"
    # Query Approperate Telemetry Table
    # Some sites have a offset, some dont
    # this queryies your Telemetry Table "GageRun"
    # with both the offset and without the offset row
    try:
        Reference_Table = pd.read_sql_query('select G_ID, File_Name, Column_Number, UTC_Offset, NumberOfHeaderRows, Instrument_Offset from '+str(Telemetry_Table)+'', conn)
    except:
        Reference_Table = pd.read_sql_query('select G_ID, File_Name, Column_Number, UTC_Offset, NumberOfHeaderRows from '+str(Telemetry_Table)+'', conn)
    for index, row in Reference_Table.iterrows():
        G_ID = Reference_Table.iloc[index, 0]
        Gauge_Name= Get_Gauge_Name(Reference_Table)
        File_Name = Reference_Table.iloc[index, 1]
        Column_Number = Reference_Table.iloc[index, 2]
        UTC_Offset = Reference_Table.iloc[index, 3]
        NumberofHeaderRows = Reference_Table.iloc[index, 4]
        # if there is an offset read this row, othersiwe there wont be a 5th row
        # the reference table has its own try/except function, it was built first, but the independence may be nice
        try:
            Offset = Reference_Table.iloc[index, 5]
        except:
            Offset = 0
        parameter_upload_data = Open_File(parameter, Gauge_Name, G_ID)
        if parameter_upload_data.empty:
            continue
        Clean_File(parameter_upload_data, Offset, parameter, Gauge_Name, G_ID)
        CHECK_CSV = parameter_upload_data.tail(1)
        parameter_upload_data = Second_Trip(parameter_upload_data)
        # set up data for sql upload
        UTC_Offset = int(UTC_Offset)
        parameter_upload_data[str(sql_prefix)+'TimeDate'] = (parameter_upload_data[str(sql_prefix)+'TimeDate'] + timedelta(hours=UTC_Offset)).dt.strftime('%m/%d/%Y %H:%M:%S.%N')
        parameter_upload_data[str(sql_prefix)+'UTCOffset'] = str(UTC_Offset)
        parameter_upload_data[str(sql_prefix)+'Est'] = "0"
        parameter_upload_data[str(sql_prefix)+'Snow'] = "0"
        parameter_upload_data[str(sql_prefix)+'Lock'] = "0"
        parameter_upload_data[str(sql_prefix)+'Warning'] = "0"
        parameter_upload_data['Provisional'] = "0"
        # use this to change the nae of the corrected table for water level etc.
        # Find Last Existing Baro Record
        cursor = conn.cursor()
        New_Trimmed = Cut_Data(parameter_table, parameter_upload_data, sql_prefix, G_ID)
        try:
            Size = New_Trimmed.shape[0]
        except:
            continue
        count = 0
        skips = 0
        for index, row in New_Trimmed.iterrows():
            try:
                cursor = conn.cursor()
                cursor.execute('INSERT INTO tblRainGauging (G_ID, '+sql_prefix+'TimeDate, '+sql_prefix+'UTCOffset, '+sql_prefix+'Value, '+sql_prefix+'Est, '+sql_prefix+'Snow, '+sql_prefix+'Lock, '+sql_prefix+'Warning, AutoDTStamp, Provisional) VALUES(?,?,?,?,?,?,?,?,?,?)', row.G_ID, row.R_TimeDate, row.R_UTCOffset, row.R_Value, row.R_Est, row.R_Snow, row.R_Lock,row.R_Warning,row.AutoDTStamp,row.Provisional)
                conn.commit()
                count = count + 1
            except:
                skips = skips + 1
                continue
        cursor.close()
        if count > 0:
            print(str(parameter)+" "+str(Gauge_Name)+" ["+str(G_ID)+"] UPLOADED "+str(Size))
        if skips > 0:
            print("    "+str(parameter)+" "+str(Gauge_Name)+" ["+str(G_ID)+"] Skipped Row "+str(skips))
        Time_Check(parameter_table, File_Path, Telemetry_Table, File_Name, CHECK_CSV, UTC_Offset, sql_prefix)
    print("Rain 15 Minutes Complete")
    print("                  ")
    print("Run Rain Tips")

    # call Rain Tips Table
    Telemetry_Table = "tblRainGageRun"
    sql_prefix = 'R_'
    # this is only used for reporting
    parameter = "Rain Tips"
    parameter_table = 'tblRainGauging5minute'
    File_Path = "W:\STS\hydro\GAUGE\zTelemetered_Tables\Cdrive_table_tips_copied"
    # Query Approperate Telemetry Table
    # Some sites have a offset, some dont
    # this queryies your Telemetry Table "GageRun"
    # with both the offset and without the offset row
    try:
        Reference_Table = pd.read_sql_query('select G_ID, File_Name_EventData, Column_Number, UTC_Offset, NumberOfHeaderRows, Instrument_Offset from '+str(Telemetry_Table)+'', conn)
    except:
        Reference_Table = pd.read_sql_query('select G_ID, File_Name_EventData, Column_Number, UTC_Offset, NumberOfHeaderRows from '+str(Telemetry_Table)+'', conn)
    for index, row in Reference_Table.iterrows():
        G_ID = Reference_Table.iloc[index, 0]
        Gauge_Name = Get_Gauge_Name(Reference_Table)
        File_Name = Reference_Table.iloc[index, 1]
        Column_Number = Reference_Table.iloc[index, 2]
        UTC_Offset = Reference_Table.iloc[index, 3]
        NumberofHeaderRows = Reference_Table.iloc[index, 4]
        # if there is an offset read this row, othersiwe there wont be a 5th row
        # the reference table has its own try/except function, it was built first, but the independence may be nice
        try:
            Offset = Reference_Table.iloc[index, 5]
        except:
            Offset = 0
        parameter_upload_data = Open_File(parameter, Gauge_Name, G_ID)
        if parameter_upload_data.empty:
            continue
        parameter_upload_data = Clean_File(parameter_upload_data, Offset, parameter, Gauge_Name, G_ID)
        # Set data for SQL upload
        parameter_upload_data[str(sql_prefix)+'TimeDate'] = (parameter_upload_data[str(sql_prefix)+'TimeDate'])
        parameter_upload_data[str(sql_prefix)+'UTCOffset'] = str(UTC_Offset)
        parameter_upload_data[str(sql_prefix)+'Est'] = "0"
        parameter_upload_data[str(sql_prefix)+'Snow'] = "0"
        parameter_upload_data[str(sql_prefix)+'Lock'] = "0"
        parameter_upload_data[str(sql_prefix)+'Warning'] = "0"
        parameter_upload_data['Provisional'] = "0"
        # use this to change the nae of the corrected table for water level etc.
        # Find Last Existing Baro Record
        cursor = conn.cursor()
        New_Trimmed = Cut_Data(parameter_table, parameter_upload_data, sql_prefix, G_ID)
        try:
            Size = New_Trimmed.shape[0]
        except:
            continue
        # Upload to SQL
        count = 0
        skips = 0
        for index, row in New_Trimmed.iterrows():
            try:
                cursor = conn.cursor()
                cursor.execute('INSERT INTO tblRainGauging5minute (G_ID ,'+sql_prefix+'Value, '+sql_prefix+'Est, '+sql_prefix+'Snow, '+sql_prefix+'Lock, AutoDTStamp,'+sql_prefix+'TimeDate,'+sql_prefix+'UTCOffset, Provisional) VALUES(?,?,?,?,?,?,?,?,?)', row.G_ID, row.R_Value, row.R_Est, row.R_Snow, row.R_Lock, row.AutoDTStamp, row.R_TimeDate, row.R_UTCOffset, row.Provisional)
                conn.commit()
                count = count + 1
            except:
                skips = skips + 1
                continue
        cursor.close()
        if count > 0:
            print(str(parameter)+" "+str(Gauge_Name)+" ["+str(G_ID)+"] UPLOADED "+str(Size))
        if skips > 0:
            print("    "+str(parameter)+" "+str(Gauge_Name)+" ["+str(G_ID)+"] Skipped Row "+str(skips))
    print("Rain Tips Complete")
    print("                  ")
    print("Run Battery")

    # call Battery Table
    Telemetry_Table = "tblBatteryRun"
    sql_prefix = 'B_'
    # this is only used for reporting
    parameter = "Battery"
    parameter_table = 'tblBatteryVoltages'
    File_Path = "W:\STS\hydro\GAUGE\zTelemetered_Tables\Cdrive_table_daily_copied"
    # Query Approperate Telemetry Table
    # Some sites have a offset, some dont
    # this queryies your Telemetry Table "GageRun"
    # with both the offset and without the offset row
    try:
        Reference_Table = pd.read_sql_query('select G_ID, File_Name, Column_Number, NumberOfHeaderRows, Instrument_Offset from '+str(Telemetry_Table)+'', conn)
    except:
        Reference_Table = pd.read_sql_query('select G_ID, File_Name, Column_Number, NumberOfHeaderRows from '+str(Telemetry_Table)+'', conn)
    # Read parameters from telemetry settings table
    for index, row in Reference_Table.iterrows():
        G_ID = Reference_Table.iloc[index, 0]
        Gauge_Name= Get_Gauge_Name(Reference_Table)
        File_Name = Reference_Table.iloc[index, 1]
        Column_Number = Reference_Table.iloc[index, 2]
        NumberofHeaderRows = Reference_Table.iloc[index, 3]
        # if there is an offset read this row, othersiwe there wont be a 5th row
        # the reference table has its own try/except function, it was built first, but the independence may be nice
        try:
            Offset = Reference_Table.iloc[index, 5]
        except:
            Offset = 0
        parameter_upload_data = Open_File(parameter, Gauge_Name, G_ID)
        if parameter_upload_data.empty:
            continue
        parameter_upload_data = Clean_File(parameter_upload_data, Offset, parameter, Gauge_Name, G_ID)
        parameter_upload_data['Voltage'] = pd.to_numeric(parameter_upload_data[str(sql_prefix)+'Value'], errors='coerce').astype("float")
        parameter_upload_data['Voltage_Date'] = parameter_upload_data[str(sql_prefix)+'TimeDate']
        cursor = conn.cursor()
        existing_data = cursor.execute("select max(Voltage_Date) from tblBatteryVoltages WHERE G_ID = "+str(G_ID)+";").fetchval()
        parameter_upload_data["Voltage_Date"] = pd.to_datetime(parameter_upload_data["Voltage_Date"])
        parameter_upload_data = parameter_upload_data[parameter_upload_data.Voltage_Date > existing_data]
        parameter_upload_data["Voltage_Date"] = parameter_upload_data["Voltage_Date"].dt.strftime('%m/%d/%Y %H:%M:%S')
        count = 0
        skips = 0
        for index, row in parameter_upload_data.iterrows():
            try:
                cursor = conn.cursor()
                cursor.execute('INSERT INTO tblBatteryVoltages (G_ID, Voltage_Date, Voltage) VALUES(?,?,?)', row.G_ID, row.Voltage_Date, row.Voltage)
                conn.commit()
                count = count + 1
            except:
                skips = skips + 1
                continue
        cursor.close()
        if count > 0:
            print(str(parameter)+" "+str(Gauge_Name)+" ["+str(G_ID)+"] UPLOADED "+str(Size))
        if skips > 0:
            print("    "+str(parameter)+" "+str(Gauge_Name)+" ["+str(G_ID)+"] Skipped Row "+str(skips))

    print("Battery Uploaded")
    print("END")

'''
def job():
    Run_Upload()

schedule.every(10).minutes.do(job)

while True:
    schedule.run_pending()
    time.sleep(1)

'''
Run_Upload()

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