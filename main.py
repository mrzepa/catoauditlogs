from os import getenv
import dotenv
from icecream import ic
import os
import urllib.request
import ssl
import json
import logging
import datetime
import socket
import sys
import time
import urllib.parse

logger = logging.getLogger(__name__)
dotenv.load_dotenv()

# send GQL query string to API, return JSON
# if we hit a network error, retry ten times with a 2 second sleep
def send(query):
    global api_call_count
    retry_count = 0
    data = {'query':query}
    headers = { 'x-api-key': api_key,'Content-Type':'application/json'}
    no_verify = ssl._create_unverified_context()
    while True:
        if retry_count > 10:
            print("FATAL ERROR retry count exceeded")
            sys.exit(1)
        try:
            request = urllib.request.Request(url='https://api.catonetworks.com/api/v1/graphql2',
                data=json.dumps(data).encode("ascii"),headers=headers)
            response = urllib.request.urlopen(request, context=no_verify, timeout=30)
            api_call_count += 1
        except Exception as e:
            logger.error(f"ERROR {retry_count}: {e}, sleeping 2 seconds then retrying")
            time.sleep(2)
            retry_count += 1
            continue
        result_data = response.read()
        if result_data[:48] == b'{"errors":[{"message":"rate limit for operation:':
            logger.error("RATE LIMIT sleeping 5 seconds then retrying")
            time.sleep(5)
            continue
        break
    result = json.loads(result_data.decode('utf-8','replace'))
    if "errors" in result:
        logger.error(f"API error: {result_data}")
        return False,result
    return True,result


if __name__ == "__main__":
    api_key = os.getenv('API_KEY')
    account_id = getenv('ACCOUNT_ID')

    api_call_count = 0
    start = datetime.datetime.now()

    timeframe = "last.PT5D"
    # API call loop
    iteration = 1
    total_count = 0
    marker = ""
    while True:
        query = '''
    {
    	auditFeed(accountIDs:[''' + account_id + '''] timeFrame:"''' + timeframe + '''" marker:"''' + marker + '''") {
    		marker
    		fetchedCount
    		hasMore
    		accounts {
    			id
    			records {
    				time
    				fieldsMap
    			}
    		}
    	}
    }'''

        logger.debug(query)
        success, resp = send(query)
        logger.debug(resp)
        if not success:
            ic(resp)
            sys.exit(1)
        audit_data = resp["data"]["auditFeed"]
        count = int(audit_data["fetchedCount"])
        total_count += count
        has_more = audit_data["hasMore"]
        marker = audit_data["marker"]
        line = f"iteration:{iteration} Count:{count} total_count:{total_count} hasMore:{has_more}"
        records = audit_data["accounts"][0]["records"]
        if len(records):
            line += f' {records[0]["time"]} {records[-1]["time"]}'
        logger.info(line)

        # print output

        for event in records:
            event["fieldsMap"]["event_timestamp"] = event["time"]
            print(json.dumps(event["fieldsMap"], indent=2, ensure_ascii=False))

        iteration += 1
        if not has_more:
            break

    end = datetime.datetime.now()
    logger.info(f"OK {total_count} events from {api_call_count} API calls in {end - start}")

