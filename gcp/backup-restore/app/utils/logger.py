from pythonjsonlogger import jsonlogger
import logging
import os
from datetime import datetime

class CustomJsonFormatter(jsonlogger.JsonFormatter):
    def __init__(self, *args, **kwargs):
        self.retry_log = kwargs.pop('retry_log', False)
        super().__init__(*args, **kwargs)

    def add_fields(self, log_record, record, message_dict):
        super(CustomJsonFormatter, self).add_fields(log_record, record, message_dict)
        log_record['timestamp'] = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
        log_record['level'] = record.levelname
        if self.retry_log:
            log_record['message'] = f"RETRY: {log_record['message']}"

def get_logger(log_level=logging.INFO):
    logger = logging.getLogger()
    log_handler = logging.StreamHandler()
    format_str = '%(level)s %(timestamp)s  %(message)s'
    formatter = CustomJsonFormatter(format_str, retry_log=os.getenv('RETRY_LOG', 'False').lower() in ('true', '1', 't'))
    log_handler.setFormatter(formatter)
    logger.addHandler(log_handler)
    logger.setLevel(log_level)
    return logger