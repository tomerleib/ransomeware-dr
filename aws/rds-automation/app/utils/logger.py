from pythonjsonlogger import jsonlogger
import logging
import os
from datetime import datetime


class CustomJsonFormatter(jsonlogger.JsonFormatter):
    def add_fields(self, log_record, record, message_dict):
        super(CustomJsonFormatter, self).add_fields(log_record, record, message_dict)
        log_record['timestamp'] = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
        log_record['level'] = record.levelname


def get_logger(log_level=logging.INFO):
    logger = logging.getLogger()
    log_handler = logging.StreamHandler()
    format_str = '%(level)s %(timestamp)s  %(message)s'
    formatter = CustomJsonFormatter(format_str)
    log_handler.setFormatter(formatter)
    logger.addHandler(log_handler)
    logger.setLevel(log_level)
    return logger