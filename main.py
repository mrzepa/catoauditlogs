import os
import sys
import json
import time
import logging
import datetime
from typing import Any, Dict, Tuple, Optional
import requests
from dotenv import load_dotenv

# Initialize logging
logging.basicConfig(
    level=logging.INFO,  # Change to DEBUG for more detailed logs
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        # Uncomment the next line to log to a file
        # logging.FileHandler("audit_log_fetcher.log")
    ]
)
logger = logging.getLogger(__name__)

# Load environment variables from .env file
load_dotenv()

# Constants
API_URL = 'https://api.catonetworks.com/api/v1/graphql2'
MAX_RETRIES = 10
RETRY_SLEEP = 2  # seconds
RATE_LIMIT_SLEEP = 5  # seconds


def send_query(query: str, api_key: str) -> Tuple[bool, Optional[Dict[str, Any]]]:
    """
    Send a GraphQL query to the Cato Networks API.

    Args:
        query (str): The GraphQL query string.
        api_key (str): The API key for authentication.

    Returns:
        Tuple[bool, Optional[Dict[str, Any]]]: A tuple where the first element is a success flag,
        and the second element is the JSON response or error details.
    """
    retry_count = 0
    headers = {
        'x-api-key': api_key,
        'Content-Type': 'application/json'
    }
    data = {'query': query}

    while retry_count <= MAX_RETRIES:
        try:
            logger.debug("Sending POST request to %s with query: %s", API_URL, query)
            response = requests.post(API_URL, headers=headers, json=data, timeout=30)
            response.raise_for_status()
            result = response.json()

            # Handle rate limiting
            if 'errors' in result:
                error_messages = [error.get('message', '') for error in result['errors']]
                logger.error("API returned errors: %s", result['errors'])

                if any('rate limit' in msg.lower() for msg in error_messages):
                    logger.warning("Rate limit encountered. Sleeping for %s seconds.", RATE_LIMIT_SLEEP)
                    time.sleep(RATE_LIMIT_SLEEP)
                    retry_count += 1
                    continue
                elif any('timeFrame' in error.get('path', []) for error in result['errors']):
                    logger.error("Error related to 'timeFrame'. Please verify the parameter value.")
                    return False, result
                else:
                    logger.error("Unhandled API errors: %s", error_messages)
                    return False, result

            return True, result

        except requests.exceptions.RequestException as e:
            logger.error("Request error on attempt %d: %s", retry_count + 1, e)
            if retry_count >= MAX_RETRIES:
                logger.critical("Max retries exceeded. Exiting.")
                sys.exit(1)
            logger.info("Retrying in %s seconds...", RETRY_SLEEP)
            time.sleep(RETRY_SLEEP)
            retry_count += 1

    logger.critical("Failed to send query after %d retries.", MAX_RETRIES)
    return False, None


def construct_query(account_id: str, timeframe: str, marker: str) -> str:
    """
    Construct the GraphQL query string.

    Args:
        account_id (str): The account ID for which to fetch audit logs.
        timeframe (str): The timeframe for the audit logs.
        marker (str): The pagination marker.

    Returns:
        str: The constructed GraphQL query.
    """
    return f'''
    {{
        auditFeed(accountIDs: ["{account_id}"] timeFrame: "{timeframe}" marker: "{marker}") {{
            marker
            fetchedCount
            hasMore
            accounts {{
                id
                records {{
                    time
                    fieldsMap
                }}
            }}
        }}
    }}
    '''


def save_logs_to_file(logs: list, output_file: str) -> None:
    """
    Save the audit logs to a JSON file.

    Args:
        logs (list): The list of audit log records.
        output_file (str): The filename to save the logs.
    """
    try:
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(logs, f, indent=4, ensure_ascii=False)
        logger.info("Successfully saved %d logs to %s.", len(logs), output_file)
    except IOError as e:
        logger.error("Failed to save logs to file: %s", e)


def fetch_audit_logs(
    api_key: str,
    account_id: str,
    timeframe: str = "PT5D",
    output_file: Optional[str] = None
) -> None:
    """
    Fetch audit logs from the Cato Networks API and save them to a file.

    Args:
        api_key (str): The API key for authentication.
        account_id (str): The account ID to fetch audit logs for.
        timeframe (str, optional): The timeframe for the logs. Defaults to "PT5D".
        output_file (Optional[str], optional): The file to save the logs. If None, prints to console.
    """
    total_count = 0
    api_call_count = 0
    iteration = 1
    marker = ""
    all_logs = []
    start_time = datetime.datetime.now()

    while True:
        query = construct_query(account_id, timeframe, marker)
        logger.debug("Sending query: %s", query)
        success, response = send_query(query, api_key)

        if not success or not response:
            logger.critical("Failed to retrieve audit logs. Exiting.")
            sys.exit(1)

        audit_data = response.get("data", {}).get("auditFeed", {})
        if not audit_data:
            logger.error("No 'auditFeed' data found in the response.")
            sys.exit(1)

        fetched_count = audit_data.get("fetchedCount", 0)
        total_count += fetched_count
        has_more = audit_data.get("hasMore", False)
        marker = audit_data.get("marker", "")
        api_call_count += 1

        logger.info(
            "Iteration %d: Fetched %d logs (Total: %d). Has more: %s",
            iteration, fetched_count, total_count, has_more
        )

        records = audit_data.get("accounts", [{}])[0].get("records", [])

        for event in records:
            fields_map = event.get("fieldsMap", {})
            fields_map["event_timestamp"] = event.get("time")
            all_logs.append(fields_map)

        if not has_more:
            break

        iteration += 1

    end_time = datetime.datetime.now()
    logger.info(
        "Completed fetching %d events using %d API calls in %s.",
        total_count, api_call_count, end_time - start_time
    )

    if output_file:
        save_logs_to_file(all_logs, output_file)
    else:
        # Print to console if no output file is specified
        for log_entry in all_logs:
            print(json.dumps(log_entry, indent=2, ensure_ascii=False))


def main():
    """
    Main function to execute the audit log fetching process.
    """
    api_key = os.getenv('API_KEY')
    account_id = os.getenv('ACCOUNT_ID')
    output_file = os.getenv('OUTPUT_FILE')  # Optional: specify via .env or environment variables
    timeframe = os.getenv('TIMEFRAME', 'PT5D')  # Optional: specify via .env or environment variables

    # Validate essential configurations
    if not api_key:
        logger.critical("API_KEY not found in environment variables.")
        sys.exit(1)
    if not account_id:
        logger.critical("ACCOUNT_ID not found in environment variables.")
        sys.exit(1)

    # Log the configurations being used
    logger.debug("Configurations - API_KEY: %s, ACCOUNT_ID: %s, OUTPUT_FILE: %s, TIMEFRAME: %s",
                 '***' if api_key else None,
                 account_id,
                 output_file,
                 timeframe)

    fetch_audit_logs(api_key, account_id, timeframe=timeframe, output_file=output_file)


if __name__ == "__main__":
    main()
