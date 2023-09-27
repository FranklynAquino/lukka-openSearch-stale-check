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
    query = ('''
                                    select j.job_id
                                    , j.entity_id
                                    , j.job_status
                                    , j.job_type
                                    ,COALESCE(j.connector_transaction_type,j.connector_blockchain_transaction_type,'N/A') transaction_type
                                    , j.last_updated_ts
                                    , (now() - j.start_ts) as duration
                                    from lco.v_all_jobs j
                                    left join account_service_sync.lco_accounts la on j.account_id = la.account_id 
                                    left join account_service_sync.lco_data_providers ldp on la.provider_id = ldp.provider_id
                                    left join account_service_sync.lco_entities le on j.entity_id = le.entity_id
                                    left join account_service_sync.lco_users u on u.user_id = j.user_id
                                    where j.job_status not in ('COMPLETED', 'FAILED','DELETED')
                                    and j.job_type in ('CONNECTOR_API','CONNECTOR_BLOCKCHAIN_API')
                                    and duration > '02:00:00'
                                    and j.account_id not in ('3fe2191d-ce15-4bb0-8d03-9136f38c2939',
                                    'dd696098-f7de-4277-8b14-303ea5a29036',
                                    'f33b1c0a-0bc2-44b9-a067-1f7817f1fee2',
                                    'cddc46de-9063-4bc8-bbbe-99a37b7071e0',
                                    'f33b1c0a-0bc2-44b9-a067-1f7817f1fee2') 
                                    and date(last_updated_ts) >= now()::date - 1
                                    order by j.last_updated_ts desc
                                    limit 200''')
    response = pcur.execute(query)
    
    list_of_opensearch_objects = []
    list_of_zombie_jobs:List[VerticaObject] = []
    
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
        
        try:
            hits = search_response['hits']['hits']
        except KeyError as e:
            list_of_zombie_jobs.append(v_object.job_id)
        
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
            else:
                list_of_zombie_jobs.append(v_object.job_id)
        
    # joined_response = ','.join(opensearch_response_list)
    
    for response in list_of_zombie_jobs:
        logger.info(f'Joined response: {response.job_id}')
        
except error.ConnectionError as e:
    logger.error(e)
    print(e)
except error.QueryError as e:
    logger.error(e)
    print(e)
finally:
    # Close all connections
    logger.info(f'Closing connection')