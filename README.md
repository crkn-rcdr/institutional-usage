# institutional-usage
Scripts to generate institutional usage statistics for the Canadiana collections. 

## Workflow Overview
The workflow of the script is illustrated in the diagram below:

![Scripts Workflow Diagram](report_scripts_diagram.png)

1. **Logs Parser (`logs_parser.py`)**:
   - The process begins with `logs_parser.py`, which reads raw log files from the server.
   - This script performs a **pre-filtering** step to retain only the logs that correspond to **page views** (i.e., requests containing "/view").
     - Each individual page from a document is counted as a separate view.
   - Any non-relevant logs (e.g., errors, requests other than page views) are excluded.
   - The output of this script is a **filtered log file**, which is a merged collection of all the log files, excluding the irrelevant "trash" lines.

2. **Usage Report Generator (`usage_report.py`)**:
   - Once the filtered logs are ready, they are passed to `usage_report.py`.
   - This script reads the filtered logs and generates an **Excel usage report**.
   - To generate the report, you must input an **institution name**.
   - The `usage_report.py` script will then load the **IP lookup table** (using `ip_parser.py`) to identify the IP addresses and proxies associated with the chosen institution.
   - Finally, the script counts the number of matching entries per day from the filtered logs, providing detailed usage data.

## Getting started

Before running this project, ensure you have the following in place:

1. **Python Environment**:
   - Ensure you have Python installed (recommended version: `Python 3.x`).
   - Install the required dependencies by running:
     ```bash
     pip install -r requirements.txt
     ```
     This will install all necessary Python packages listed in the `requirements.txt` file.

2. **IP Address Lookup File**:
   - Obtain the latest version of the licensing team's IP Addresses excel file. This file serves as a lookup table for IP addresses, proxies, and institutions.
   - Keep the file path handy, as it is required for the usage_report.py script to run.

3. **Institutions File**:
   - Make sure that the institutions.txt file is up to date and that it matches the list of institutions in the IP Addresses excel file. 

## Usage Overview
