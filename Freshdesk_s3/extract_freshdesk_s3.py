import requests
import dateutil.parser
import pandas as pd
from dateutil.relativedelta import relativedelta
from business_duration import businessDuration
from datetime import time,datetime
import holidays as pyholidays
import pytz
import json
import logging
from sys import stdout
import sys
from helper_functions import BusinessHoursTable,CalculateFirstResponseTime,CalculateResolutionTime,GetResolutionStatus,Dataframe_to_s3,ChangeDateToLocalTimeZone
from freshdesk_class import API
import os 
import boto3
from io import BytesIO,TextIOWrapper
import time
import gzip

# Define logging for the function
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
if logger.handlers:
    logger.handlers = []
stream_handler = logging.StreamHandler(stdout)
stream_handler.setFormatter(formatter)
logger.addHandler(stream_handler)

def ExtractFreshdeskDataToS3(event,context):
    # Get the lamba environment variables
    api_key = os.environ['API']
    domain = os.environ['DOMAIN']
    months = os.environ['MONTHS']
    months = int(months)
    source_tz = os.environ['SOURCE_TZ']
    dest_tz = os.environ['DEST_TZ']
    bucketName = os.environ['FRESHDESK_BUCKET']
    file_format = os.environ['FILE_FORMAT']
    timestr = time.strftime("%Y%m%d%H%M%S")
    s3_resource  = boto3.resource('s3')

    # Call the API class to get the api details 
    api = API(domain, api_key, months)

    # Business hours table
    Bus_Hrs=BusinessHoursTable(api_key)

    #----------------------------------------------------
    # Retrieve the Freshdesk Ticket information
    #----------------------------------------------------
    api.tickets.createDf()
    api.tickets.extract_tickets()
    df_Tickets = api.tickets.get_AllTicketDetails()

    # Change the timezone from utc to Singapore timezone
    df_Tickets['due_by'] = df_Tickets.apply(lambda row: ChangeDateToLocalTimeZone(row['due_by'],source_tz,dest_tz), axis=1)
    df_Tickets['fr_due_by'] = df_Tickets.apply(lambda row: ChangeDateToLocalTimeZone(row['fr_due_by'],source_tz,dest_tz), axis=1)
    df_Tickets['created_at'] = df_Tickets.apply(lambda row: ChangeDateToLocalTimeZone(row['created_at'],source_tz,dest_tz), axis=1)
    df_Tickets['updated_at'] = df_Tickets.apply(lambda row: ChangeDateToLocalTimeZone(row['updated_at'],source_tz,dest_tz), axis=1)
    df_Tickets['nr_due_by'] = df_Tickets.apply(lambda row: ChangeDateToLocalTimeZone(row['nr_due_by'],source_tz,dest_tz), axis=1)
    df_Tickets['scheduled_datetime'] = df_Tickets.apply(lambda row: ChangeDateToLocalTimeZone(row['scheduled_datetime'],source_tz,dest_tz), axis=1)
    df_Tickets['agent_responded_at'] = df_Tickets.apply(lambda row: ChangeDateToLocalTimeZone(row['agent_responded_at'],source_tz,dest_tz), axis=1)
    df_Tickets['requester_responded_at'] = df_Tickets.apply(lambda row: ChangeDateToLocalTimeZone(row['requester_responded_at'],source_tz,dest_tz), axis=1)
    df_Tickets['first_responded_at'] = df_Tickets.apply(lambda row: ChangeDateToLocalTimeZone(row['first_responded_at'],source_tz,dest_tz), axis=1)
    df_Tickets['status_updated_at'] = df_Tickets.apply(lambda row: ChangeDateToLocalTimeZone(row['status_updated_at'],source_tz,dest_tz), axis=1)
    df_Tickets['reopened_at'] = df_Tickets.apply(lambda row: ChangeDateToLocalTimeZone(row['reopened_at'],source_tz,dest_tz), axis=1)
    df_Tickets['resolved_at'] = df_Tickets.apply(lambda row: ChangeDateToLocalTimeZone(row['resolved_at'],source_tz,dest_tz), axis=1)
    df_Tickets['closed_at'] = df_Tickets.apply(lambda row: ChangeDateToLocalTimeZone(row['closed_at'],source_tz,dest_tz), axis=1)
    df_Tickets['pending_since'] = df_Tickets.apply(lambda row: ChangeDateToLocalTimeZone(row['pending_since'],source_tz,dest_tz), axis=1)

    # Get the first response time, resolution time and resolution status
    df_Tickets['first_response_time_hrs'] = df_Tickets.apply(lambda row: CalculateFirstResponseTime(row['created_at'],row['first_responded_at'],row['country'],Bus_Hrs), axis=1)
    df_Tickets['resolution_time_hrs'] = df_Tickets.apply(lambda row: CalculateResolutionTime(row['created_at'],row['resolved_at'],row['country'],Bus_Hrs), axis=1)
    df_Tickets['resolution_status'] = df_Tickets.apply(lambda row: GetResolutionStatus(row['resolved_at'],row['due_by']),axis=1)

    # Log Error if Ticket is not retrieved
    logger.debug(" Shape of the tickets retrieved  :%d X %d", *df_Tickets.shape)
    if df_Tickets.empty:
        logger.warning("No tickets retieved from the freshdesk. Stop processing")
        sys.exit()
    
    # Store the file to the Freshdesk S3 bucket
    fileName = 'Tbl_Tickets_'+timestr+'.'+file_format+'.gz'
    Dataframe_to_s3(s3_resource, df_Tickets, bucketName, fileName, file_format)
    
    #----------------------------------------------------
    # Retrieve the Freshdesk Agent information
    #----------------------------------------------------
    api.agents.createDf()
    api.agents.extract_agents()
    df_Agents = api.agents.get_AllAgentDetails()

    # Change the timezone from utc to Singapore timezone
    df_Agents['created_at'] = df_Agents.apply(lambda row: ChangeDateToLocalTimeZone(row['created_at'],source_tz,dest_tz), axis=1)
    df_Agents['updated_at'] = df_Agents.apply(lambda row: ChangeDateToLocalTimeZone(row['updated_at'],source_tz,dest_tz), axis=1)
    df_Agents['last_active_at'] = df_Agents.apply(lambda row: ChangeDateToLocalTimeZone(row['last_active_at'],source_tz,dest_tz), axis=1)
    df_Agents['last_login_at'] = df_Agents.apply(lambda row: ChangeDateToLocalTimeZone(row['last_login_at'],source_tz,dest_tz), axis=1)

    # Log Error if Agent is not retrieved
    logger.debug(" Shape of the agent retrieved  :%d X %d", *df_Agents.shape)
    if df_Agents.empty:
        logger.warning("No agent retieved from the freshdesk. Stop processing")
        sys.exit()
    
    # Store the file to the Freshdesk S3 bucket
    fileName = 'Tbl_Agents_'+timestr+'.'+file_format+'.gz'
    Dataframe_to_s3(s3_resource, df_Agents, bucketName, fileName, file_format)

    #----------------------------------------------------
    # Retrieve the Freshdesk Group information
    #----------------------------------------------------
    api.groups.createDf()
    api.groups.extract_groups()
    df_Groups = api.groups.get_AllGroupDetails()

     # Change the timezone from utc to Singapore timezone
    df_Groups['created_at'] = df_Groups.apply(lambda row: ChangeDateToLocalTimeZone(row['created_at'],source_tz,dest_tz), axis=1)
    df_Groups['updated_at'] = df_Groups.apply(lambda row: ChangeDateToLocalTimeZone(row['updated_at'],source_tz,dest_tz), axis=1)

    # Log Error if Group is not retrieved
    logger.debug(" Shape of the group retrieved  :%d X %d", *df_Groups.shape)
    if df_Groups.empty:
        logger.warning("No group retieved from the freshdesk. Stop processing")
        sys.exit()
    
    # Store the file to the Freshdesk S3 bucket
    fileName = 'Tbl_Groups_'+timestr+'.'+file_format+'.gz'
    Dataframe_to_s3(s3_resource, df_Groups, bucketName, fileName, file_format)

    #----------------------------------------------------
    # Retrieve the Freshdesk Survey information
    #----------------------------------------------------           
    api.surveys.createDf()
    api.surveys.extract_surveys()
    df_Survey = api.surveys.get_AllSurveyDetails()

    # Change the timezone from utc to Singapore timezone
    df_Survey['created_at'] = df_Survey.apply(lambda row: ChangeDateToLocalTimeZone(row['created_at'],source_tz,dest_tz), axis=1)
    df_Survey['updated_at'] = df_Survey.apply(lambda row: ChangeDateToLocalTimeZone(row['updated_at'],source_tz,dest_tz), axis=1)

    # Log Error if Survey is not retrieved
    logger.debug(" Shape of the survey retrieved  :%d X %d", *df_Survey.shape)
    if df_Survey.empty:
        logger.warning("No survey retieved from the freshdesk. Stop processing")
        sys.exit()

    # Store the file to the Freshdesk S3 bucket
    fileName = 'Tbl_Survey_'+timestr+'.'+file_format+'.gz'
    Dataframe_to_s3(s3_resource, df_Survey, bucketName, fileName, file_format)
