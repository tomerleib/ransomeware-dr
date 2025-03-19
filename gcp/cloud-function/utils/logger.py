from pythonjsonlogger import jsonlogger
from coralogix.constants import Coralogix
from coralogix.handlers import CoralogixLogger
import logging
import os 
from datetime import datetime

Coralogix.CORALOGIX_LOG_URL = 'https://logs.coralogix.com/v1/logs'
APP_NAME = os.getenv('APP_NAME', 'gcp-retention')
SUB_SYSTEM = os.getenv('SUB_SYSTEM', 'gcp')

class CustomJsonFormatter(jsonlogger.JsonFormatter):
    def add_fields(self, log_record, record, message_dict):
        super(CustomJsonFormatter, self).add_fields(log_record, record, message_dict)
        log_record['timestamp'] = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
        log_record['level'] = record.levelname
        
def get_logger(level):
    logger = logging.getLogger()
    if not logger.handlers:
        # StreamHandler for stdout
        stream_handler = logging.StreamHandler()
        stream_formatter = logging.Formatter('%(levelname)s - %(message)s')
        stream_handler.setFormatter(stream_formatter)
        logger.addHandler(stream_handler)

        # CoralogixLogger handler
        private_key = os.getenv('CORALOGIX_PRIVATE_KEY')
        coralogix_handler = CoralogixLogger(private_key, APP_NAME, SUB_SYSTEM)
        json_formatter = CustomJsonFormatter('%(levelname)s - %(message)s')
        coralogix_handler.setFormatter(json_formatter)
        logger.addHandler(coralogix_handler)

        logger.setLevel(level)
    return logger
