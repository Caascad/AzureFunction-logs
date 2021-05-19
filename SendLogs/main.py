from typing import List
import json
import logging

import azure.functions as func


def main(events: List[func.EventHubEvent]):
    for event in events:
        evt = json.loads(event
        .get_body().decode('utf-8'))
        for record in evt['records']:
            logging.info(':'+record['properties']['log'])
