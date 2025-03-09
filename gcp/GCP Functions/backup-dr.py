import logging
import os
from concurrent.futures import ThreadPoolExecutor
from utils.common import list_instances, process_instance, process_instance_retention, handle_futures
from utils.logger import get_logger
from datetime import timedelta

from cloudevents.http import CloudEvent
import functions_framework

import time

days = os.getenv('RETENTION_DAYS', 1)
project = 'production'


@functions_framework.cloud_event
def retention(cloud_event: CloudEvent):
    get_logger('INFO')
    start = time.time()
    instances = list_instances(project)

    with ThreadPoolExecutor(max_workers=25) as executor:
        futures = []
        for instance in instances:
            future = executor.submit(process_instance, project, instance, 'start')
            futures.append(future)
        handle_futures(futures)
    time.sleep(10)

    with ThreadPoolExecutor(max_workers=20) as executor:
        delete_futures = []
        for instance in instances:
            delete_futures.append(executor.submit(process_instance_retention, project, instance, days))
        handle_futures(delete_futures)

    with ThreadPoolExecutor(max_workers=20) as executor:
        futures = []
        for instance in instances:
            future = executor.submit(process_instance, project, instance, 'stop')
            futures.append(future)
        handle_futures(futures)

    end = time.time()
    elapsed_time = timedelta(seconds=end - start)
    logging.info(f"Total time for retention: {elapsed_time}")