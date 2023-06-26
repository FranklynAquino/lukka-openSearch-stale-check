import json
from myUtils import env
from myUtils import get_logger
from datetime import (datetime, 
                    timedelta)

logger = get_logger('OpenSearch class process: payload setup ')

class SearchBodySetup:
    def __init__(self):
        self.search_body:dict = json.load(open(f'{env.resource_path}/api/json_templates/opensearch_template.json'))
        self.time_now:datetime = datetime.utcnow()
        self.past_time:datetime
        
    def set_time_now_back(self,minutes=0):
        self.time_now = self.time_now - timedelta(minutes=minutes)
        
    def choose_explicit_query(self, json_template_name=''):
        self.search_body = json.load(open(f'{env.resource_path}/api/json_templates/{json_template_name}.json'))

    def set_time_range(self,mins_before):
        date_now = self.time_now.strftime('%Y-%m-%d')
        time_now = self.time_now.strftime('%H:%M:%S.%f')
        time_now = time_now[:-3]
        
        self.past_time = self.time_now - timedelta(minutes=int(mins_before))
        past_date = self.past_time.strftime('%Y-%m-%d')
        past_time = self.past_time.strftime('%H:%M:%S.%f')
        past_time = past_time[:-3]

        search_time_range:dict = self.search_body['query']['bool']['filter'][0]['range']['@timestamp']
        search_time_range.update([('gte',f'{past_date}T{past_time}'),('lte',f'{date_now}T{time_now}')])
        logger.info(f'Your current search times: {past_date}T{past_time} - {date_now}T{time_now}')


    # def set_query(self, arg1='',search_type=''):
    #     search_match_1:dict = self.search_body['query']['bool']['filter'][1]['multi_match']
    #     search_match_1.update([('query',arg1)])
    #     search_match_1.update([('type',search_type)])
    #     search_match_1.update([('lenient',True)])
        
    #     # search_match_2:dict = self.search_body['query']['bool']['filter'][1]['multi_match']
    #     # arg2 = f"'{arg2}'"
    #     # search_match_2.update([('query',arg2)])
    #     # search_match_2.update([('type',type)])
    #     # search_match_2.update([('lenient',True)])
        
    #     logger.info(f'Your current search query: {self.search_body}')
    #     return self.search_body
    
    def set_match_query(self, arg1=''):
        search_match_1:dict = self.search_body['query']['bool']['filter'][1]['match_phrase']
        search_match_1.update([('log_processed.message',arg1)])
    
    def get_search_body(self):
        return self.search_body
    
