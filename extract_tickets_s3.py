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
from settings import apiconfig, timezone
from helper_functions import ChangeDateToLocalTimeZone,BusinessHoursTable,CalculateFirstResponseTime,CalculateResolutionTime,GetResolutionStatus
from freshdesk_class import API

formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
if logger.handlers:
    logger.handlers = []
stream_handler = logging.StreamHandler(stdout)
stream_handler.setFormatter(formatter)
logger.addHandler(stream_handler)

# Retrive the parameters
api_key = apiconfig['API']
domain = apiconfig['DOMAIN']
months = apiconfig['MONTH']
source_tz = timezone['SOURCE_TZ']
dest_tz = timezone['DEST_TZ']

if __name__ == "__main__":

    api = API(domain, api_key,months)
    api.tickets.createDf()
    api.tickets.extract_tickets()
    df_Tickets = api.tickets.get_AllTicketDetails()

    # Business hours table
    Bus_Hrs=BusinessHoursTable(api_key)

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
                
