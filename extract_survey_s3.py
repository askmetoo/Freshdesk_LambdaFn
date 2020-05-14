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