from time import sleep
from opensearchpy import OpenSearch
from api.process.opensearch_body_setup import SearchBodySetup
from myUtils import get_logger


logger = get_logger('OpenSearch Endpoints')

class OpenSearchEndpoints:
    def __init__(self, host_name, port_number, opensearch_user, opensearch_pass):
        self.host_name = host_name
        self.port_number = port_number
        self.opensearch_user = opensearch_user
        self.opensearch_pass = opensearch_pass
        
        self.client = OpenSearch(
        hosts = [{"host": self.host_name, "port": self.port_number}],
        http_auth = (self.opensearch_user,self.opensearch_pass),
        http_compress = True,
        use_ssl = True,
        verify_certs = False,
        ssl_assert_hostname = False,
        ssl_show_warn = False)
        
        self.search_setup = SearchBodySetup()
        
        self.max_tries = 5
        self.tries = 0
    
    def set_payload(self,arg1,mins_before,opensearch_search_type):
        self.search_setup.set_time_range(mins_before=mins_before)
        arg1 = f'{arg1}'
        self.search_setup.set_query(arg1=arg1,search_type=opensearch_search_type)
        
    def get_custom_search_body(self):
        logger.info(f'Your current search body: {self.search_setup.search_body}')
        return self.search_setup.get_search_body()
    
    def run_search(self,source='',filter=None,size=10):
        while self.tries < self.max_tries:
            try:
                if len(source) == 0 and filter is None:
                    response:dict = self.client.search(
                        index='fluent-bit*',
                        body=self.search_setup.search_body,
                        size=size
                    )
                elif len(source) > 0 and filter is not None:
                    response:dict = self.client.search(
                        index='fluent-bit*',
                        body=self.search_setup.search_body,
                        filter_path=filter,
                        _source=[source],
                        size=size
                    )
                elif filter is not None:
                    response:dict = self.client.search(
                        index='fluent-bit*',
                        body=self.search_setup.search_body,
                        filter_path=[*filter],
                        size=size
                    )
                elif len(source) == 0:
                    response:dict = self.client.search(
                        index='fluent-bit*',
                        body=self.search_setup.search_body,
                        _source=[source],
                        size=size
                    )
                return response
            except Exception as e:
                logger.info(f"Error: {e}. Retrying in 5 seconds...")
                sleep(5)
                self.tries += 1
                logger.info(f'Failed to connect after {self.tries} retries.')
    
    def create_url(self,last_applied_ver_id,query_arg:str):
        formatted_query_arg = query_arg.replace('','%20')
        k_url_query=f'"{last_applied_ver_id}"%20and%20"{formatted_query_arg}"'
        kibana_url = (f"https://kibana.lukka.tech/_dashboards/app/"
                    + f"discover#/?_g=(filters:!(),refreshInterval:(pause:!t,value:0),"
                    + f"time:(from:now-120m,to:now))&_a=(columns:!(log_processed.message),"
                    + f"filters:!(),index:"
                    + f"'39ee7400-f8f4-11ec-a94d-dfad9877c6d5',"
                    + f"interval:auto,query:(language:kuery,query:'{k_url_query}'),sort:!())")
        return kibana_url
    
    def create_custom_url(self,query_arg:str,additional_args=None):
        formatted_query_arg = query_arg.replace('','%20')
        k_url_query=f'"{formatted_query_arg}"%20AND%20"{additional_args}"'
        kibana_url = (f"https://kibana.lukka.tech/_dashboards/app/"
                    + f"discover#/?_g=(filters:!(),refreshInterval:(pause:!t,value:0),"
                    + f"time:(from:now-120m,to:now))&_a=(columns:!(log_processed.message),"
                    + f"filters:!(),index:"
                    + f"'39ee7400-f8f4-11ec-a94d-dfad9877c6d5',"
                    + f"interval:auto,query:(language:kuery,query:'{k_url_query}'),sort:!())")
        return kibana_url