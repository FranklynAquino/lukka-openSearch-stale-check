from typing import List
import vertica_python
import vertica_python.errors as error
from myUtils import (env, get_logger)
from api.process.opensearch_endpoints import OpenSearchEndpoints
from api.vertica_object import VerticaObject
from api.opensearch_response_object import OpenSearchResponseObject


logger = get_logger('Main Run')

logger.info("Connecting to DB...")
try:
    pcon = vertica_python.connect(host=env.db_host, port=env.db_port, user=env.db_uname, password=env.db_pwd, database=env.db_name)
    logger.info(f'Connected to {env.db_host}:{env.db_port}/{env.db_name}')
    
    pcur = pcon.cursor()
    logger.info(f'Starting the query....')
    query = '''
            select j.job_id
            from lco.jobs j
            left join account_service_sync.lco_accounts la on j.account_id = la.account_id 
            left join account_service_sync.lco_data_providers ldp on la.provider_id = ldp.provider_id 
            left join account_service_sync.lco_entities le on j.entity_id = le.entity_id
            left join account_service_sync.lco_users u on u.user_id = j.user_id
            where j.error_message ilike CONCAT('%',CONCAT('stale','%'))
            and date(last_updated_ts) > now()::date - 7
            order by last_updated_ts desc
            limit 50'''
    response = pcur.execute(query)
    
    list_of_opensearch_objects = []
    
    #Call Kibana/opensearch one at a time.
    for row in response.iterate():
        vertica_object:VerticaObject = VerticaObject(job_id=row[0])
        list_of_opensearch_objects.append(vertica_object)
    
    opensearch = OpenSearchEndpoints(env.opensearch_host_name, env.opensearch_port_number, 
                            env.opensearch_username, env.opensearch_password)
    
    opensearch_response_list:List[OpenSearchResponseObject] = []
    
    for v_object in list_of_opensearch_objects:
        v_object:VerticaObject = v_object
        opensearch.search_setup.set_match_query(v_object.job_id)
        opensearch.search_setup.set_time_range(mins_before=env.opensearch_set_mins_before)
        search_response:dict = opensearch.run_search(size=1)
        # logger.info(search_response)
        
        hits = search_response['hits']['hits']
        
        for hit in hits:
            if any(x for x in hit['_source']):
                try:
                    logger.info(f'Serializing hits from response')
                    opensearch_response:OpenSearchResponseObject = OpenSearchResponseObject(timestamp=hit['_source']['time'],
                                                                        label_app_name=hit['_source']['kubernetes']['labels']['app'],
                                                                        pod_name=hit['_source']['kubernetes']['pod_name'],
                                                                        job_id=v_object.job_id)
                except KeyError as e:
                    logger.info(f'KeyError found -> {e}')
                finally:
                    opensearch_response_list.append(opensearch_response)
        
    # joined_response = ','.join(opensearch_response_list)
    
    for response in opensearch_response_list:
        logger.info(f'Joined response: {response.to_string()}')
        
except error.ConnectionError as e:
    logger.error(e)
    print(e)
except error.QueryError as e:
    logger.error(e)
    print(e)
finally:
    # Close all connections
    logger.info(f'Closing connection')