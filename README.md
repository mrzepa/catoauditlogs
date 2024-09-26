# Cato Audit Log Exporter

A Python script to fetch, process, and export audit logs from the Cato Networks API into human-readable formats such as text and CSV files. This tool simplifies the retrieval and analysis of audit logs, making it easier for administrators and analysts to monitor and review changes within their Cato Networks environment.

## Table of Contents

- [Features](#features)
- [Prerequisites](#prerequisites)
- [Installation](#installation)
- [Configuration](#configuration)
- [Usage](#usage)
  - [Command-Line Arguments](#command-line-arguments)
  - [Examples](#examples)
- [License](#license)

## Features

- **Fetch Audit Logs:** Retrieve audit logs from the Cato Networks API for specified timeframes.
- **Timestamp Conversion:** Convert millisecond timestamps to human-readable UTC datetime strings.
- **Nested Field Reconstruction:** Transform flat JSON keys with dot notation into nested dictionaries for enhanced readability.
- **Flexible Output Formats:** Export logs as either a human-readable text file or a structured CSV file.
- **Command-Line Interface:** Utilize command-line arguments for flexible and dynamic usage.
- **Environment Variable Configuration:** Configure settings via a `.env` file for ease of use and security.
- **Robust Error Handling:** Implements retries and handles API rate limiting gracefully.

## Prerequisites

- **Python Version:** Python 3.12 or higher.
- **Python Packages:** The following Python packages are required:
  - `requests`
  - `python-dotenv`
  - `pandas`
  - `argparse`

## Installation

1. **Clone the Repository**

   ```bash
   git clone https://github.com/mrzepa/catoauditlogs.git
   cd catoauditlogs

## Create a Virtual Environment (Optional but Recommended)
```bash
    python3 -m venv venv
    source venv/bin/activate  # On Windows: venv\Scripts\activate
```
## Install Required Packages

```bash
    pip install -r requirements.txt
```
If requirements.txt is not provided, install the packages manually:
```bash
    pip install requests python-dotenv pandas argparse
```

# Configuration
The script uses environment variables for configuration. You can set these variables either through a .env file or by passing them as command-line arguments.

## 1. Create a .env File
In the root directory of the project, create a file named .env and add the following variables:

```env
Copy code
API_KEY=your_cato_api_key
ACCOUNT_ID=your_account_id
OUTPUT_FILE=readable_audit_logs.txt  # Or readable_audit_logs.csv
SAVE_AS_CSV=False  # Set to True to save as CSV
```
**Variable Descriptions:**
- **API_KEY:** Your Cato Networks API key.
- **ACCOUNT_ID:** The account ID for which you want to fetch audit logs.
- **OUTPUT_FILE:** The filename where you want to save the audit logs. Use .txt for a human-readable text file or .csv for a structured CSV file.
- **TIMEFRAME:** The timeframe for the audit logs in ISO8601 duration format (e.g., P2D for 2 days).
- **SAVE_AS_CSV:** Set to True to save logs as a CSV file; otherwise, logs will be saved as a text file.

## 2. Ensure .env is in .gitignore
```gitignore
.env
```
# Usage
You can run the script using command-line arguments, environment variables, or a combination of both.

## Command-Line Arguments
| Argument | Description | Required | Default |
|:---------|:------------|:---------|:--------|
| --api_key | Cato Networks API key. | No       | .env |
| --account_id | Account ID to fetch audit logs for. | No       | .env |
| --output_file | File to save the audit logs. | No       | .env |
| --timeframe | Timeframe for audit logs (e.g., last.P2D for 2 days). | No       | last.P2D |
| --save_as_csv | Flag to save logs as CSV instead of text. | No       | False |

# Examples
**1. Using Environment Variables Only**
Ensure your .env file is properly configured, then run:
```bash
python main.py
```

**2. Using Command-Line Arguments**
Override .env variables by providing arguments directly:
```bash
python main.py --api_key YOUR_API_KEY --account_id YOUR_ACCOUNT_ID --output_file logs.csv --timeframe P2D --save_as_csv
```

**3. Mixing .env and Command-Line Arguments**
Use the .env file for some variables and command-line arguments for others:
```bash
python main.py --save_as_csv
```
This command uses API_KEY, ACCOUNT_ID, OUTPUT_FILE, and TIMEFRAME from the .env file but saves the logs as a CSV file.

# License

This project is licensed under the MIT License https://chatgpt.com/c/LICENSE.