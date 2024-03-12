import logging

def setup_logger():
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    return logger
