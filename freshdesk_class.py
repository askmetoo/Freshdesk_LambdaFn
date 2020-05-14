import logging
from sys import stdout
import sys
import requests
import pandas as pd
from business_duration import businessDuration
from datetime import time,datetime
from dateutil.relativedelta import relativedelta

formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
if logger.handlers:
    logger.handlers = []
stream_handler = logging.StreamHandler(stdout)
stream_handler.setFormatter(formatter)
logger.addHandler(stream_handler)

class Tickets(object):
    def __init__(self, api, months):
        self._api = api
        self._months = months
        
    def createDf(self):
        self.TicketsDf = pd.DataFrame(data = None)
        return self.TicketsDf

    def extract_tickets(self, **kwargs):
        """List all tickets, optionally filtered by a view. Specify filters as
        keyword arguments, such as:'new_and_my_open', 'watching', 'deleted'
        filter_name = one of ['new_and_my_open', 'watching', 'deleted']
        """
        logger.info("Preparation of the url for the api scrapping")
        filter_name = ''
        rolling_months = datetime.now() + relativedelta(months=self._months)
        datetime_since = rolling_months.replace(day=1,hour=0,minute=0,second=0)
        datetime_since = datetime_since.strftime("%Y-%m-%dT%H:%M:%SZ")
      
        # Build the url for the api scrapping
        if filter_name :
            url = 'tickets?filter=%s' % filter_name
            url = url + '&include=requester,stats,description&updated_since='+datetime_since
        else:
            url = 'tickets/?include=requester,stats,description&updated_since='+datetime_since
            
        logger.info("The Freshdesk url: {}".format(url))
        page = 1
        per_page = 100
        tickets = []
        
        # Skip pagination by looping over each page and adding tickets
        while True:
            this_page = self._api._get(url + '&page=%d&per_page=%d'
                                       % (page, per_page), kwargs)
            tickets += this_page
            if len(this_page) < per_page:
                break
            page += 1
        
        # load the  tickets data from the freshdesk 
        load_tickets = [t for t in tickets]
        df_tickets = pd.DataFrame(load_tickets)
        
        logger.debug(" Shape of the Tickets retrieved  :%d X %d", *df_tickets.shape)
        if df_tickets.empty:
            logger.warning("No tickets retieved from the freshdesk. Stop processing")
            sys.exit()
        
        #Replace the priority, status and source columns with respective naming objects specified by freshdesk
        df_tickets=df_tickets.replace({'priority' : {1: 'Low', 2: 'Medium', 3: 'High', 4: 'Urgent'}})
        df_tickets=df_tickets.replace({'status' : {2: 'Open', 3: 'Pending', 4: 'Resolved', 5: 'Closed'}})
        df_tickets=df_tickets.replace({'source':{ 1: 'Email', 2: 'Portal', 3: 'Phone', 4: 'Forum', 5: 'Twitter',6: 'Facebook', 7: 'Chat'}})
        
        #Convert the tags column from list to string 
        df_tickets['tags_string'] = [','.join(map(str, t)) for t in df_tickets['tags']]
        
        #------------------------------------------------------
        #Extend the custom fields to seperate columns
        #------------------------------------------------------
        L_Custom_fields=[]
        for dic in df_tickets['custom_fields']:
            L = [v for k,v in dic.items()]
            L_Custom_fields.append(L)
            
            
        df_Custom_fields = pd.DataFrame.from_records(L_Custom_fields,columns=['cf_test_subcategory'
                                                                              ,'cf_country' 
                                                                              ,'cf_members_name'
                                                                              ,'cf_cluster'
                                                                              ,'cf_contract_number'
                                                                              ,'cf_building'
                                                                              ,'cf_space'
                                                                              ,'cf_common_area'
                                                                              ,'cf_category'
                                                                              ,'cf_subcategory'
                                                                              ,'cf_scheduled_datetime'
                                                                              ,'cf_test_category'
                                                                              ,'cf_bill_back'
                                                                              ,'cf_amounttobill_lcy'
                                                                              ,'cf_billing_status'
                                                                              ,'cf_fsm_contact_name'
                                                                              ,'cf_fsm_phone_number'
                                                                              ,'cf_fsm_service_location'
                                                                              ,'cf_fsm_appointment_start_time'
                                                                              ,'cf_fsm_appointment_end_time'])
        logger.debug(" Shape of the Tickets custom fields retrieved  :%d X %d", *df_Custom_fields.shape)       
        if df_Custom_fields.empty:
            logger.warning("No ticket custom fields retieved from the freshdesk. Stop processing")
            sys.exit()
            
        #------------------------------------------------------
        #Extend the requestor fields to seperate columns
        #------------------------------------------------------
        L_Requestor=[]
        for dic in df_tickets['requester']:
            L = [v for k,v in dic.items()]
            L_Requestor.append(L)
            
        df_Requestor = pd.DataFrame.from_records(L_Requestor,columns=['id'
                                                                      ,'name' 
                                                                      ,'email'
                                                                      ,'mobile'
                                                                      , 'phone'])
        df_Requestor = df_Requestor.rename(columns={'id': 'requestor_id'})
        
        logger.debug(" Shape of the Tickets requestors retrieved  :%d X %d", *df_Requestor.shape)       
        if df_Requestor.empty:
            logger.warning("No ticket requestor fields retieved from the freshdesk. Stop processing")
            sys.exit()
            
        #------------------------------------------------------
        #Extend the stat fields to seperate columns
        #------------------------------------------------------
        L_Stats=[]
        for dic in df_tickets['stats']:
            L = [v for k,v in dic.items()]
            L_Stats.append(L)
            
        df_Stats = pd.DataFrame.from_records(L_Stats,columns=['agent_responded_at'
                                                             ,'requester_responded_at' 
                                                             ,'first_responded_at'
                                                             ,'status_updated_at'
                                                             ,'reopened_at'
                                                             ,'resolved_at'
                                                             ,'closed_at'
                                                             ,'pending_since'])
        
        logger.debug(" Shape of the Tickets stats retrieved  :%d X %d", *df_Stats.shape)       
        if df_Stats.empty:
            logger.warning("No ticket stats fields retieved from the freshdesk. Stop processing")
            sys.exit()
       
        #--------------------------------------------------------------------------------------
        #Concat tickets, custom fields, requestor and stats dataframes and rename the columns
        #--------------------------------------------------------------------------------------
        df_tickets = pd.concat([df_tickets, df_Custom_fields,df_Requestor,df_Stats], axis=1, sort=False)
        
        df_tickets = df_tickets.rename(columns={'id': 'TicketId', 'cf_country': 'country',
                                                'cf_members_name': 'members_name','cf_cluster':'cluster',
                                                'cf_contract_number':'contract_number','cf_building':'building',
                                                'cf_space':'space','cf_common_area':'common_area',
                                                'cf_category':'category','cf_subcategory':'subcategory',
                                                'cf_scheduled_datetime':'scheduled_datetime','cf_bill_back':'bill_back',
                                                'cf_amounttobill_lcy':'amounttobill_lcy',
                                                'cf_billing_status':'billing_status','tags_string':'tags'})
        
        df_tickets = df_tickets[['TicketId', 'fr_escalated', 'spam','email_config_id', 'group_id', 'priority',
                                 'responder_id', 'source', 'company_id', 'status','subject', 'product_id', 
                                 'type','due_by', 'fr_due_by', 'is_escalated', 'created_at','updated_at', 
                                 'associated_tickets_count', 'tags', 'description_text','nr_due_by', 
                                 'nr_escalated', 'country', 'members_name', 'cluster', 'contract_number',
                                 'building', 'space', 'common_area', 'category','subcategory', 
                                 'scheduled_datetime', 'bill_back', 'amounttobill_lcy', 'billing_status',
                                 'requestor_id', 'name', 'email', 'mobile', 'phone','agent_responded_at', 
                                 'requester_responded_at', 'first_responded_at','status_updated_at', 'reopened_at', 
                                 'resolved_at', 'closed_at','pending_since']]
        
        self.TicketsDf = df_tickets     
        return self.TicketsDf
    
    def list_new_and_my_open_tickets(self):
        """List all new and open tickets."""
        return self.list_tickets(filter_name='new_and_my_open')

    def list_watched_tickets(self):
        """List watched tickets, closed or open."""
        return self.list_tickets(filter_name='watching')

    def list_deleted_tickets(self):
        """Lists all deleted tickets."""
        return self.list_tickets(filter_name='deleted')
    
    def get_AllTicketDetails(self):
        return self.TicketsDf

class Agents(object):
    def __init__(self, api):
        self._api = api
        
    def createDf(self):
        self.AgentsDf = pd.DataFrame(data = None)
        return self.AgentsDf

    def extract_agents(self,**kwargs):
        
        url = 'agents'
        page = 1
        per_page = 100
        agents = []
        
        # Skip pagination by looping over each page and adding agents
        while True:
            this_page = self._api._get(url + '?page=%d&per_page=%d'
                                       % (page, per_page), kwargs)
            agents += this_page
            if len(this_page) < per_page:
                break
            page += 1
        
        load_agents = [agent for agent in agents]
        df_agents = pd.DataFrame(load_agents)
        logger.debug(" Shape of the Agents retrieved  :%d X %d", *df_agents.shape)
        if df_agents.empty:
            logger.warning("No Agents retieved from the freshdesk. Stop processing")
            sys.exit()
        
        #------------------------------------------------------
        #Extend the agent contact fields to seperate columns
        #------------------------------------------------------
        L_Contact=[]
        for dic in df_agents['contact']:
            L = [v for k,v in dic.items()]
            L_Contact.append(L)
            
        df_contact = pd.DataFrame.from_records(L_Contact,columns=['active'
                                                                  ,'email' 
                                                                  ,'job_title'
                                                                  ,'language'
                                                                  ,'last_login_at'
                                                                  ,'mobile'
                                                                  ,'name'
                                                                  ,'phone'
                                                                  ,'time_zone'
                                                                  ,'created_at'
                                                                  ,'updated_at'])
        logger.debug(" Shape of the Agent contacts retrieved  :%d X %d", *df_contact.shape)
        if df_contact.empty:
            logger.warning("No Agent contacts retieved from the freshdesk. Stop processing")
            sys.exit()
        
        #------------------------------------------------------------
        #Concat agents, contacts dataframes and rename the columns
        #------------------------------------------------------------      
        df_agents = pd.concat([df_agents, df_contact], axis=1, sort=False)
        
        df_agents = df_agents.rename(columns={'id':'agent_id'})
        
        df_agents = df_agents[['agent_id','available', 'occasional', 'ticket_scope', 'created_at', 'updated_at', 
                               'last_active_at', 'available_since', 'type','active', 'email', 'job_title',
                               'language', 'last_login_at', 'mobile', 'name', 'phone', 'time_zone']]
        
        
        self.AgentsDf = df_agents       
        return self.AgentsDf
        
    def get_AllAgentDetails(self):
        return self.AgentsDf
   
class Groups(object):
    def __init__(self, api):
        self._api = api
        
    def createDf(self):
        self.GroupDf = pd.DataFrame(data = None)
        return self.GroupDf

    def extract_groups(self,**kwargs):
        
        url = 'groups'
        page = 1
        per_page = 100
        groups = []
        
        # Skip pagination by looping over each page and adding groups
        while True:
            this_page = self._api._get(url + '?page=%d&per_page=%d'
                                       % (page, per_page), kwargs)
            groups += this_page
            if len(this_page) < per_page:
                break
            page += 1
        
        load_groups = [group for group in groups]
        df_groups = pd.DataFrame(load_groups)
        logger.debug(" Shape of the Groups retrieved  :%d X %d", *df_groups.shape)
        if df_groups.empty:
            logger.warning("No Groups retieved from the freshdesk. Stop processing")
            sys.exit()
        
        df_groups = df_groups.rename(columns={'id':'group_id'})
        
        self.GroupsDf = df_groups       
        return self.GroupsDf
        
    def get_AllGroupDetails(self):
        return self.GroupsDf
      
class Survey(object):
    def __init__(self, api):
        self._api = api
        
    def createDf(self):
        self.SurveyDf = pd.DataFrame(data = None)
        return self.SurveyDf

    def extract_surveys(self,**kwargs):
        
        url = 'surveys/satisfaction_ratings'
        page = 1
        per_page = 100
        surveys = []
        
        # Skip pagination by looping over each page and adding groups
        while True:
            this_page = self._api._get(url + '?page=%d&per_page=%d'
                                       % (page, per_page), kwargs)
            surveys += this_page
            if len(this_page) < per_page:
                break
            page += 1
        
        load_surveys = [survey for survey in surveys]
        df_surveys = pd.DataFrame(load_surveys)
        logger.debug(" Shape of the Survey retrieved  :%d X %d", *df_surveys.shape)
        if df_surveys.empty:
            logger.warning("No Surveys retieved from the freshdesk. Stop processing")
            sys.exit()
               
        #------------------------------------------------------
        #Extend the rating field to seperate columns
        #------------------------------------------------------
        L_Rating=[]
        for dic in df_surveys['ratings']:
            L = [v for k,v in dic.items()]
            L_Rating.append(L)
            
        df_rating = pd.DataFrame.from_records(L_Rating,columns=['default_question'])
        logger.debug(" Shape of the ratings retrieved  :%d X %d", *df_rating.shape)
        if df_rating.empty:
            logger.warning("No Ratings contacts retieved from the freshdesk. Stop processing")
            sys.exit()
        
        #------------------------------------------------------------
        #Concat survey, ratings dataframes and rename the columns
        #------------------------------------------------------------      
        df_surveys = pd.concat([df_surveys, df_rating], axis=1, sort=False)
        
        df_surveys = df_surveys.rename(columns={'default_question':'rating'})
        
        df_surveys = df_surveys.drop('ratings',axis=1)
        
        #Replace the rating with respective naming objects specified by freshdesk
        df_surveys= df_surveys.replace({'rating' : {100: 'Neutral', 101: 'Happy', 102: 'Very Happy' 
                                                       ,103: 'Extremely Happy',-101: 'Unhappy'
                                                       ,-102: 'Very Unhappy', -103: 'Extremely Unhappy'}})
         
        
        
        self.SurveyDf = df_surveys       
        return self.SurveyDf
        
    def get_AllSurveyDetails(self):
        return self.SurveyDf

class API(object):
    def __init__(self, domain, api_key,months):
        """Creates a wrapper to perform API actions.
        Arguments:
          domain:    the Freshdesk domain. i.e hmlet
          api_key:   the API key
          months: No of months of data needs to be extracted
        Instances:
          .tickets:  the Ticket API
          .agents : the Agent API
          .groups : the Group API
          .surveys: the Survey API
        """

        self._api_prefix = 'https://{}/api/v2/'.format(domain.rstrip('/'))
        self._session = requests.Session()
        self._session.auth = (api_key, 'unused_with_api_key')
        self._session.headers = {'Content-Type': 'application/json'}
        
        self.tickets = Tickets(self,months)
        self.agents  = Agents(self)
        self.groups  = Groups(self)
        self.surveys = Survey(self)

        if domain.find('freshdesk.com') < 0:
            logger.error('Problem with Freshdesk v2 API')
            raise AttributeError('Freshdesk v2 API works only via Freshdesk'
                                 'domains and not via custom CNAMEs')
        self.domain = domain

    def _action(self, req):
        try:
            j = req.json()
        except:
            req.raise_for_status()
            j = {}

        if 'error' in j:
            logger.error('Problem with Freshdesk v2 API')
            raise HTTPError('{}: {}'.format(j.get('description'),
                                            j.get('errors')))

        # Catch any other errors
        try:
            req.raise_for_status()
        except Exception as e:
            logger.error('Failed on request')
            raise HTTPError("{}: {}".format(e, j))

        return j
    
    def _get(self, url, params={}):
        """Wrapper around request.get() to use the API prefix. Returns a JSON response."""
        req = self._session.get(self._api_prefix + url, params=params)
        return self._action(req)

    def _post(self, url, data={}):
        """Wrapper around request.post() to use the API prefix. Returns a JSON response."""
        req = self._session.post(self._api_prefix + url, data=data)
        return self._action(req)
