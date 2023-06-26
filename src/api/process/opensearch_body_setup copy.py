import json
from myUtils import get_logger
from datetime import (datetime, 
                    timedelta)
from .multi_match_object import MultiMatchObject

logger = get_logger('OpenSearch class process: payload setup ')

class SearchBodySetup:
    def __init__(self):
        self.search_body:dict = json.load(open('./api/json_templates/opensearch_template.JSON'))
        

    def set_time_range(self,mins_before):
        x:datetime = datetime.utcnow()
        date_now = x.strftime('%Y-%m-%d')
        time_now = x.strftime('%H:%M:%S.%f')
        time_now = time_now[:-3]
        
        now_x_mins_before = x - timedelta(minutes=int(mins_before))
        past_date = now_x_mins_before.strftime('%Y-%m-%d')
        past_time = now_x_mins_before.strftime('%H:%M:%S.%f')
        past_time = past_time[:-3]

        search_time_range:dict = self.search_body['query']['bool']['filter'][3]['range']['@timestamp']
        search_time_range.update([('gte',f'{past_date}T{past_time}'),('lte',f'{date_now}T{time_now}')])
        logger.info(f'Your current search times: {past_date}T{past_time} - {date_now}T{time_now}')
        
        ##TODO: One by one call
        
    # def package_multi_match(self,multi_match_list:list):
    #     opensearch_filter:list = self.search_body['query']['bool']['filter']
    #     opensearch_filter.extend(multi_match_list)
    #     logger.info(f'\nYour current Multi Match Object List INCOMING: {multi_match_list}\n\n')
    #     logger.info(f'\nYour current Multi Match Object List OUTGOING: {opensearch_filter}\n\n')
        
    # def construct_multi_match(self,query):
    #     multi_match:MultiMatchObject = MultiMatchObject()
    #     return multi_match.get_multi_match_object(query)
    
    def set_query(self, arg1='',type=''):
        search_match_1:dict = self.search_body['query']['bool']['filter'][1]['multi_match']
        search_match_1.update([('query',arg1)])
        search_match_1.update([('type',type)])
        search_match_1.update([('lenient',True)])
        
        # search_match_2:dict = self.search_body['query']['bool']['filter'][1]['multi_match']
        # arg2 = f"'{arg2}'"
        # search_match_2.update([('query',arg2)])
        # search_match_2.update([('type',type)])
        # search_match_2.update([('lenient',True)])
        
        logger.info(f'Your current search query: {self.search_body}')
        return self.search_body
    

        
        # search_match_2:dict = self.search_body['query']['bool']['filter'][1]['multi_match']
        # arg2 = f"'{arg2}'"
        # search_match_2.update([('query',arg2)])
        # search_match_2.update([('type',type)])
        # search_match_2.update([('lenient',True)])
        
        logger.info(f'Your current search query: {self.search_body}')
        return self.search_body
    
    def get_search_body(self):
        return self.search_body
    
    def choose_explicit_query(self, json_template_name=''):
        self.search_body = json.load(open(f'api/json_templates/{json_template_name}.json'))