import logging
import os
from dotenv import load_dotenv

class Env:
    def __init__(self):
        load_dotenv()
        self.db_host = os.environ['PDB_HOST']
        self.db_port = os.environ['DB_PORT']
        self.db_name = os.environ['DBNAME']
        self.db_uname = os.environ['DB_UNAME']
        self.db_pwd = os.environ['DB_PWD']
        self.resource_path = os.environ['RESOURCE_PATH']
        self.opensearch_host_name = os.environ['OPENSEARCH_HOST_NAME']
        self.opensearch_port_number = os.environ['OPENSEARCH_PORT_NUMBER']
        self.opensearch_username = os.environ['OPENSEARCH_USERNAME']
        self.opensearch_password = os.environ['OPENSEARCH_PASSWORD']
        self.opensearch_set_mins_before = os.environ['OPENSEARCH_SET_MINS_BEFORE']
        
        
def get_logger(name):
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    # create a logging format
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    # add the console handler
    consoleHandler = logging.StreamHandler()
    consoleHandler.setFormatter(formatter)
    consoleHandler.setFormatter(formatter)
    # add handler
    logger.addHandler(consoleHandler)
    return logger

env = Env()