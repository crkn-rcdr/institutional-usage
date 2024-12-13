# institutional-usage
Scripts to generate institutional usage statistics for the Canadiana collections. 

## Workflow Overview
The workflow of the script is illustrated in the diagram below:

![Scripts Workflow Diagram](report_scripts_diagram.png)

1. **Logs Parser** (`logs_parser.py`):
   - The process begins with `logs_parser.py`, which reads raw log files from the server.
   - This script performs a **pre-filtering** step to retain only the logs that correspond to **page views** (i.e., requests containing "/view").
     - Each individual page from a document is counted as a separate view.
   - Any non-relevant logs (e.g., errors, requests other than page views) are excluded.
   - The output of this script is a **filtered log file**, which is a merged collection of all the log files, excluding the irrelevant "trash" lines.

2. **Usage Report Generator** (`usage_report.py`):
   - Once the filtered logs are ready, they are passed to `usage_report.py`.
   - This script reads the filtered logs and generates an **Excel usage report**.
   - To generate the report, you must input an **institution name**.
   - The `usage_report.py` script will then load the **IP lookup table** (using `ip_parser.py`) to identify the IP addresses and proxies associated with the chosen institution.
   - Finally, the script counts the number of matching entries per day from the filtered logs, providing detailed usage data.

## Getting started

1. **Clone the repository**:
   If you haven't already, clone the repository to your local machine:
   ```bash
   git clone <repository-url>
   ```

2. **Set up your environment**:
   - Make a copy of the `.env.sample` file and rename it to `.env`:
     ```bash
     cp .env.sample .env
     ```
   - Open the newly created `.env` file and replace `<your-server-name>` with the appropriate server name.

3. **Install dependencies**:
   - Ensure you have Python installed (recommended version: `Python 3.x`).
   - Install the required dependencies by running:
     ```bash
     pip install -r requirements.txt
     ```
     This will install all necessary Python packages listed in the `requirements.txt` file.

4. **Prepare the required files**:
   - Obtain the latest version of the licensing team's **IP Addresses** excel file and the `institutions.txt` file. Ensure that `institutions.txt` is up to date and matches the list of institutions in the **IP Addresses** excel file.

## Usage Overview
