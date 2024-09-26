import os
import sys
import json
import time
import logging
from typing import Any, Dict, Tuple, Optional
import requests
from dotenv import load_dotenv
import argparse
from datetime import datetime, timezone
import pandas as pd

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
    return '''
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


def convert_timestamp(ms_timestamp: str) -> str:
    """
    Convert a timestamp in milliseconds to a human-readable UTC datetime string.

    Args:
        ms_timestamp (str): Timestamp in milliseconds as a string.

    Returns:
        str: Human-readable datetime string in UTC, formatted as 'YYYY-MM-DD HH:MM:SS'.
    """
    try:
        # Convert string to integer
        ms = int(ms_timestamp)
        # Convert milliseconds to seconds
        s = ms / 1000
        # Create timezone-aware datetime object in UTC
        dt = datetime.fromtimestamp(s, timezone.utc)
        # Format datetime
        return dt.strftime('%Y-%m-%d %H:%M:%S UTC')
    except Exception as e:
        logger.error(f"Error converting timestamp {ms_timestamp}: {e}")
        return ms_timestamp  # Return original if conversion fails


def unflatten_dict(d: Dict[str, Any], sep: str = '.') -> Dict[str, Any]:
    """
    Reconstruct nested dictionaries from flat dictionaries with dot-separated keys.

    Args:
        d (Dict[str, Any]): The flat dictionary.
        sep (str): Separator used in keys.

    Returns:
        Dict[str, Any]: The reconstructed nested dictionary.
    """
    result = {}
    for key, value in d.items():
        parts = key.split(sep)
        current = result
        for part in parts[:-1]:
            if part not in current:
                current[part] = {}
            current = current[part]
        current[parts[-1]] = value
    return result


def generate_log_summary(log: Dict[str, Any]) -> str:
    """
    Generate a human-readable summary of a log entry.

    Args:
        log (Dict[str, Any]): The log entry as a dictionary.

    Returns:
        str: A formatted string summarizing the log.
    """
    # Convert timestamps
    creation_date = convert_timestamp(log.get('creation_date', ''))
    insertion_date = convert_timestamp(log.get('insertion_date', ''))
    event_timestamp = log.get('event_timestamp', '')

    # Reconstruct nested fields
    nested_log = unflatten_dict(log)

    # Extract key information
    admin = nested_log.get('admin', 'Unknown Admin')
    admin_id = nested_log.get('admin_id', 'Unknown ID')
    change_type = nested_log.get('change_type', 'Unknown Change Type')
    model_type = nested_log.get('model_type', 'Unknown Model Type')
    model_name = nested_log.get('model_name', 'Unknown Model Name')
    module = nested_log.get('module', 'Unknown Module')

    # Start building the summary
    summary = f"**Event Timestamp:** {event_timestamp}\n"
    summary += f"**Admin:** {admin} (ID: {admin_id})\n"
    summary += f"**Change Type:** {change_type}\n"
    summary += f"**Model Type:** {model_type}\n"
    summary += f"**Model Name:** {model_name}\n"
    summary += f"**Module:** {module}\n"
    summary += f"**Creation Date:** {creation_date}\n"
    summary += f"**Insertion Date:** {insertion_date}\n"

    # Add details about changes
    change_after = nested_log.get('change', {}).get('After', {})
    change_before = nested_log.get('change', {}).get('Before', {})

    if change_after:
        summary += "**Changes After:**\n"
        summary += json.dumps(change_after, indent=4) + "\n"
    if change_before:
        summary += "**Changes Before:**\n"
        summary += json.dumps(change_before, indent=4) + "\n"

    summary += "-" * 50 + "\n"

    return summary


def save_logs_to_file(logs: list, output_file: str) -> None:
    """
    Save the audit logs as a human-readable text file.

    Args:
        logs (list): The list of audit log records.
        output_file (str): The filename to save the logs.
    """
    try:
        with open(output_file, 'w', encoding='utf-8') as f:
            for log in logs:
                summary = generate_log_summary(log)
                f.write(summary)
        logger.info("Successfully saved %d logs to %s.", len(logs), output_file)
    except IOError as e:
        logger.error("Failed to save logs to file: %s", e)


def save_logs_to_csv(logs: list, output_file: str) -> None:
    """
    Save the audit logs to a CSV file.

    Args:
        logs (list): The list of audit log records.
        output_file (str): The filename to save the logs.
    """
    # Convert logs to pandas DataFrame
    df = pd.DataFrame(logs)

    # Convert timestamp fields
    if 'creation_date' in df.columns:
        df['creation_date'] = df['creation_date'].apply(convert_timestamp)
    if 'insertion_date' in df.columns:
        df['insertion_date'] = df['insertion_date'].apply(convert_timestamp)
    if 'event_timestamp' in df.columns:
        # Optionally, format event_timestamp
        df['event_timestamp'] = pd.to_datetime(df['event_timestamp']).dt.strftime('%Y-%m-%d %H:%M:%S')

    try:
        df.to_csv(output_file, index=False)
        logger.info("Successfully saved %d logs to %s.", len(logs), output_file)
    except IOError as e:
        logger.error("Failed to save logs to CSV file: %s", e)


def fetch_audit_logs(
        api_key: str,
        account_id: str,
        timeframe: str = "P2D",
        output_file: Optional[str] = None,
        save_as_csv: bool = False
) -> None:
    """
    Fetch audit logs from the Cato Networks API and save them to a readable file.

    Args:
        api_key (str): The API key for authentication.
        account_id (str): The account ID to fetch audit logs for.
        timeframe (str, optional): The timeframe for the logs. Defaults to "P2D".
        output_file (Optional[str], optional): The file to save the logs. If None, prints to console.
        save_as_csv (bool, optional): Whether to save the logs as CSV. Defaults to False.
    """
    total_count = 0
    api_call_count = 0
    iteration = 1
    marker = ""
    all_logs = []
    start_time = datetime.now()

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

    end_time = datetime.now()
    logger.info(
        "Completed fetching %d events using %d API calls in %s.",
        total_count, api_call_count, end_time - start_time
    )

    if output_file:
        if save_as_csv:
            save_logs_to_csv(all_logs, output_file)
        else:
            save_logs_to_file(all_logs, output_file)
    else:
        # Print to console if no output file is specified
        for log_entry in all_logs:
            summary = generate_log_summary(log_entry)
            print(summary)


def parse_arguments() -> argparse.Namespace:
    """
    Parse command-line arguments.

    Returns:
        argparse.Namespace: The parsed arguments.
    """
    parser = argparse.ArgumentParser(description="Fetch and format audit logs from Cato Networks API.")
    parser.add_argument('--api_key', type=str, help='Cato Networks API key.')
    parser.add_argument('--account_id', type=str, help='Account ID to fetch audit logs for.')
    parser.add_argument('--output_file', type=str, help='File to save the audit logs.')
    parser.add_argument('--timeframe', type=str, default='last.P2D', help='Timeframe for audit logs (e.g., last.P2D for 2 days).')
    parser.add_argument('--save_as_csv', action='store_true', help='Save logs as CSV instead of text.')
    return parser.parse_args()


def main():
    """
    Main function to execute the audit log fetching process.
    """
    args = parse_arguments()

    api_key = args.api_key or os.getenv('API_KEY')
    account_id = args.account_id or os.getenv('ACCOUNT_ID')
    output_file = args.output_file or os.getenv('OUTPUT_FILE')
    timeframe = args.timeframe

    save_as_csv = args.save_as_csv or os.getenv('SAVE_AS_CSV', 'False').lower() in ['true', '1', 'yes']

    # Validate essential configurations
    if not api_key:
        logger.critical("API_KEY not found. Provide it via --api_key or in the .env file.")
        sys.exit(1)
    if not account_id:
        logger.critical("ACCOUNT_ID not found. Provide it via --account_id or in the .env file.")
        sys.exit(1)

    # Log the configurations being used
    logger.debug("Configurations - API_KEY: %s, ACCOUNT_ID: %s, OUTPUT_FILE: %s, TIMEFRAME: %s, SAVE_AS_CSV: %s",
                 '***' if api_key else None,
                 account_id,
                 output_file,
                 timeframe,
                 save_as_csv)

    fetch_audit_logs(api_key, account_id, timeframe=timeframe, output_file=output_file, save_as_csv=save_as_csv)


if __name__ == "__main__":
    main()
