from datetime import datetime
from typing import List
import json
import os

import requests
import azure.functions as func

AKS_CATEGORIES = ['kube-scheduler', 'kube-controller-manager',
                  'cluster-autoscaler', 'kube-audit', 'kube-apiserver',
                  'kube-audit-admin', 'guard', 'AllMetrics']

AKS_LOG_TIME_PATTERN = '%Y-%m-%dT%H:%M:%S.%f0Z'
DEFAULT_LOG_TIME_PATTERN = '%Y-%m-%dT%H:%M:%SZ'


def ms_in_ns() -> int:
    return 1000 * 1000 * 1000


def convert_time_to_timestamp(time: str, pattern: str) -> str:
    timestamp = datetime.strptime(time, pattern).timestamp()
    timestamp = timestamp * ms_in_ns()
    return f'{timestamp:.0f}'


def main(events: List[func.EventHubEvent]):
    eventhub = os.getenv('EventHubName')
    functionapp = os.getenv('FUNCTIONAPP_NAME')
    url = os.getenv('PROMTAIL_ENDPOINT')

    headers = {
        'Content-type': 'application/json'
    }
    payload = {
        'streams': []
    }

    streams = {}

    for event in events:
        evt = json.loads(event
                         .get_body().decode('utf-8'))

        for record in evt['records']:
            resourceId = record['resourceId']

            if record['category'] in AKS_CATEGORIES:
                line = record['properties']['log']
                timestamp = convert_time_to_timestamp(
                    record['time'], AKS_LOG_TIME_PATTERN)
            else:
                line = record['properties']['message']
                timestamp = convert_time_to_timestamp(
                    record['time'], DEFAULT_LOG_TIME_PATTERN)

            if resourceId in streams:
                streams[resourceId]['values'].append([timestamp, line])
            else:
                streams[resourceId] = {
                    'stream': {
                        '__azure_functionapp': functionapp,
                        '__azure_eventhub': eventhub,
                        '__azure_resourceId': resourceId
                    },
                    'values': [[timestamp, line]]
                }

    payload['streams'] = list(streams.values())

    try:
        r = requests.post(url=url, headers=headers, data=json.dumps(payload))
        r.raise_for_status()
    except requests.exceptions.RequestException as e:
        raise SystemExit(e)
