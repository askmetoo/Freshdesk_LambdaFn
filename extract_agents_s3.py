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
from helper_functions import ChangeDateToLocalTimeZone,BusinessHoursTable,CalculateFirstResponseTime,CalculateResolutionTime,GetResolutionStatus,DecryptKeys
from freshdesk_class import API

formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
if logger.handlers:
    logger.handlers = []
stream_handler = logging.StreamHandler(stdout)
stream_handler.setFormatter(formatter)
logger.addHandler(stream_handler)

source_tz = timezone['SOURCE_TZ']
dest_tz = timezone['DEST_TZ']

if __name__ == "__main__":
    # Decrypt the security keys
    d_keys = DecryptKeys(apiconfig)
    api_key = d_keys['api']
    domain = d_keys['domain']
    months = d_keys['month']

    api = API(domain, api_key,months)            
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
