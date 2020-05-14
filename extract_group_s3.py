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

api_key = apiconfig['API']
domain = apiconfig['DOMAIN']
months = apiconfig['MONTH']
source_tz = timezone['SOURCE_TZ']
dest_tz = timezone['DEST_TZ']

if __name__ == "__main__":

    api = API(domain, api_key,months)
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
