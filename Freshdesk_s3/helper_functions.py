import requests
import dateutil.parser
import pandas as pd
from business_duration import businessDuration
from datetime import time,datetime
import holidays as pyholidays
import pytz
import json
import logging
from sys import stdout
import sys
from io import BytesIO,TextIOWrapper
import boto3
import gzip

formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
if logger.handlers:
    logger.handlers = []
stream_handler = logging.StreamHandler(stdout)
stream_handler.setFormatter(formatter)
logger.addHandler(stream_handler)

#------------------------------------------------------------------------------------
# Build the Business Hours table
#Country_TimeZone	Timezone	        Start_Hour	End_Hour	Start_Min	End_Min
#	Sydney	        Australia/Sydney	08	        17	        00      	00
#	Hong Kong	    Asia/Hong_Kong	    08	        17	        00	        00
#	Tokyo	        Asia/Tokyo	        08	        17	        00	        00
#	Singapore	    Asia/Singapore  	09	        18	        00	        00
#------------------------------------------------------------------------------------    
def BusinessHoursTable(api_key):

    r = requests.get("https://hmlet.freshdesk.com/api/v2/business_hours", auth = (api_key, ''))

    if r.status_code == 200:
        data = r.text
        Df_buss_hrs = pd.read_json(data, orient='records')

    else:
        logger.error ("Failed to read tickets, errors are displayed below,")
        response = json.loads(r.content)
        logger.error ("x-request-id : " + r.headers['x-request-id'])
        logger.error ("Status Code : " + str(r.status_code))
    
    Bussiness_Hours = []
    for i, row in Df_buss_hrs.iterrows():
        buss_hrs = row['business_hours']
        country_tz = row['time_zone']
        start_t = buss_hrs['monday']['start_time']
        end_t = buss_hrs['monday']['end_time']
        start_am_pm = buss_hrs['monday']['start_time'][5:7]
        end_am_pm = buss_hrs['monday']['end_time'][5:7]
        
        if country_tz == "Sydney":
            country_timezones = 'Australia/Sydney'
        elif country_tz == "Hong Kong":
            country_timezones = 'Asia/Hong_Kong'
        elif country_tz == "Tokyo":
            country_timezones = 'Asia/Tokyo'
        elif country_tz == "Singapore":
            country_timezones = 'Asia/Singapore'
        else:
            country_timezones = 'Asia/Singapore'
            
        
        if start_am_pm == "pm" or end_am_pm == "pm":
            s_in_time = datetime.strptime(start_t, "%I:%M %p")
            start_t = datetime.strftime(s_in_time, "%H:%M %p")
            e_in_time = datetime.strptime(end_t, "%I:%M %p")
            end_t = datetime.strftime(e_in_time, "%H:%M %p")
            
        start_hr = start_t[0:2]
        end_hr = end_t[0:2]
        start_min = start_t[3:5]
        end_min = end_t[3:5]
        start_am_pm = start_t[6:8]
        end_am_pm = end_t[6:8]
        
        temp_bhrs = [country_tz,country_timezones,start_hr,end_hr,start_min,end_min]
        Bussiness_Hours.append(temp_bhrs)

    Df_Bhrs = pd.DataFrame(Bussiness_Hours,columns=['Country_TimeZone','Timezone','Start_Hour','End_Hour','Start_Min','End_Min'])
    
    logger.debug(" Shape of the Business hours table :%d X %d", *Df_Bhrs.shape)
    if Df_Bhrs.empty:
        logger.warning("Business hours table not built. Stop processing")

    return Df_Bhrs

#------------------------------------------------------------------------------------------------
# Calculate the first response time.
# First_response_time_hrs = first_response_time - created_dt (excluding the non business hours)
#-------------------------------------------------------------------------------------------------
def CalculateFirstResponseTime(created_dt,first_responded_dt,Country,Buss_hrs_df):
    
    try:
        # If the first_responded_dt is not found, populate first_response_time_hrs with 0.00
        if first_responded_dt == '' or pd.isnull(first_responded_dt) or first_responded_dt is None :
            first_response_time_hrs = 0.00
        else:
    
            Df_business_hrs = Buss_hrs_df.copy()
            date_tz = 'Asia/Singapore'
            
            Df_business_hrs['Country_TimeZone'] = Df_business_hrs['Country_TimeZone'].replace('Sydney', 'Australia')
            Df_business_hrs['Country_TimeZone'] = Df_business_hrs['Country_TimeZone'].replace('Tokyo', 'Japan')

            if (Country == "HK" or Country == "HKG"):
                Cntry = 'Hong Kong'
            elif (Country == "SG" or Country == "SIN"):
                Cntry = 'Singapore'
            else:
                Cntry = Country

            Start_Hour=9
            Start_Min=0
            End_Hour=18
            End_Min=0
            Cntry_tz='Singapore'

            for i, row in Df_business_hrs.iterrows(): 
                if Cntry == row['Country_TimeZone'] : 
                    Start_Hour=int(row['Start_Hour'])
                    Start_Min=int(row['Start_Min'])
                    End_Hour=int(row['End_Hour'])
                    End_Min=int(row['End_Min'])
                    Cntry_tz=Cntry
                    if Cntry_tz != "Singapore" :
                        created_dt = ChangeDateToLocalTimeZone(created_dt,date_tz,row['Timezone'])
                        first_responded_dt = ChangeDateToLocalTimeZone(first_responded_dt,date_tz,row['Timezone'])  
                    break

            starttime=time(Start_Hour,Start_Min,0)
            endtime=time(End_Hour,End_Min,0) 

            if Cntry_tz == 'Hong Kong':
                Cntry_tz = 'HongKong'
            
            # Public holiday list of the country
            holidaylist = pyholidays.CountryHoliday(Cntry_tz)
            unit='hour'
            first_response_time_hrs = 0.00
            #By default weekends are Saturday and Sunday
            
            created_dt=datetime.strptime(created_dt, '%Y-%m-%d %H:%M:%S')
            first_responded_dt=datetime.strptime(first_responded_dt, '%Y-%m-%d %H:%M:%S')
            first_response_time = (businessDuration(created_dt,first_responded_dt,starttime,endtime,holidaylist=holidaylist,unit=unit))
            
            if first_response_time == '' or pd.isnull(first_response_time):
                first_response_time_hrs = 0.00
            else:
                first_response_time_hrs = first_response_time/24
        
        return first_response_time_hrs  
    except:
        logger.error("First Response time cannot be computed. Please check")

#------------------------------------------------------------------------------------------------
# Calculate the resolution time.
# Resolution_time_hrs = resolution_tim - created_dt (excluding the non business hours)
#-------------------------------------------------------------------------------------------------
def CalculateResolutionTime(created_dt,resolved_dt,Country,Buss_hrs_df):
    
    try:
        # If the resolved_dt is not found, populate resolution_time_hrs with 0.00
        if resolved_dt == '' or pd.isnull(resolved_dt) or resolved_dt is None :
            resolution_time_hrs = 0.00
        else:
        
            Df_business_hrs = Buss_hrs_df.copy()
            date_tz = 'Asia/Singapore'

            Df_business_hrs['Country_TimeZone'] = Df_business_hrs['Country_TimeZone'].replace('Sydney', 'Australia')
            Df_business_hrs['Country_TimeZone'] = Df_business_hrs['Country_TimeZone'].replace('Tokyo', 'Japan')

            if (Country == "HK" or Country == "HKG" or Country == "Hong Kong"):
                Cntry = 'Hong Kong'
            elif (Country == "SG" or Country == "SIN"):
                Cntry = 'Singapore'
            else:
                Cntry = Country

            Start_Hour=9
            Start_Min=0
            End_Hour=18
            End_Min=0
            Cntry_tz='Singapore'

            for i, row in Df_business_hrs.iterrows():   
                if Cntry == row['Country_TimeZone'] : 
                    Start_Hour=int(row['Start_Hour'])
                    Start_Min=int(row['Start_Min'])
                    End_Hour=int(row['End_Hour'])
                    End_Min=int(row['End_Min'])
                    Cntry_tz=Cntry
                    if Cntry_tz != "Singapore" :
                        created_dt = ChangeDateToLocalTimeZone(created_dt,date_tz,row['Timezone'])
                        resolved_dt = ChangeDateToLocalTimeZone(resolved_dt,date_tz,row['Timezone'])              
                    break

            starttime=time(Start_Hour,Start_Min,0)
            endtime=time(End_Hour,End_Min,0) 

            if Cntry_tz == 'Hong Kong':
                Cntry_tz = 'HongKong'

            holidaylist = pyholidays.CountryHoliday(Cntry_tz)
            unit='hour'

            resolution_time_hrs = 0.00

            #By default weekends are Saturday and Sunday
            created_dt=datetime.strptime(created_dt, '%Y-%m-%d %H:%M:%S')
            resolved_dt=datetime.strptime(resolved_dt, '%Y-%m-%d %H:%M:%S')
            resolution_time = (businessDuration(created_dt,resolved_dt,starttime,endtime,holidaylist=holidaylist,unit=unit))
            if resolution_time == '' or pd.isnull(resolution_time):
                resolution_time_hrs = 0.00
            else:
                resolution_time_hrs = resolution_time/24

        return resolution_time_hrs
    except:
        logger.error("Resolution time cannot be computed. Please check")

#------------------------------------------------------------------------------------------------
# Calculate the resolution status.
# Resolution_status :
# If resolution_dt > resolution_due_dt ---> SLA Violated
# If resolution_dt <= resolution_due_dt ---> Within SLA
#-------------------------------------------------------------------------------------------------
def GetResolutionStatus(resolution_dt,resolution_due_dt):

    try:
        sla_violation_flag = ''
        if ( (resolution_dt == '' or pd.isnull(resolution_dt) or resolution_dt is None) or 
                (resolution_due_dt == '' or pd.isnull(resolution_due_dt) or resolution_due_dt is None) ):
            sla_violation_flag = ''
        elif resolution_dt > resolution_due_dt:
            sla_violation_flag = 'SLA Violated'
        else: 
            sla_violation_flag = 'Within SLA'
        return sla_violation_flag 
    except:
        logger.info("Resolution Status not found. Please check")  

#------------------------------------------------------------------------------------------------
# Copy the dataframe to S3 files.
#-------------------------------------------------------------------------------------------------
def Dataframe_to_s3(s3_resource, input_datafame, bucket_name, fileName, format):
    if format == 'csv':
        gz_buffer = BytesIO()
        with gzip.GzipFile(mode='w', fileobj=gz_buffer) as gz_file:
            input_datafame.to_csv(TextIOWrapper(gz_file, 'utf8'), index=False)
    
    s3_resource.Object(bucket_name, fileName).put(Body=gz_buffer.getvalue())
    logger.info("Copied the dataframe to AWS S3 bucket")

#---------------------------------------------------------------------------
# Change the timezone from utc to respective country timezone.
#---------------------------------------------------------------------------
def ChangeDateToLocalTimeZone(dt,tz_s,tz_d):
    try:
        dt = dt.replace("T", " ")
        dt = dt.replace("Z", "")
        localFormat = "%Y-%m-%d %H:%M:%S"
        dt=datetime.fromisoformat(dt)
        utc = pytz.timezone(tz_s)
        Singapore_tz = pytz.timezone(tz_d)
        loc_dt = utc.localize(dt)
        loc_tz = loc_dt.astimezone(Singapore_tz)
        Singapore_dt=loc_tz.strftime(localFormat)
        return Singapore_dt
    except:
        None