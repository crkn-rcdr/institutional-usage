
from log_processor import logs_to_df, filter_logs
from ip_parser import process_ip_file
import pandas as pd
import argparse

def count_views(log_file, inst_ips):
    """
    Counts the number of views for a specific institution based on logs.

    This function reads a log file, filters the log data based on IP addresses associated with an institution, 
    and counts the number of accesses for that institution. It then groups the data by month and day, and sorts the 
    results chronologically.

    Parameters:
    log_file (str): Path to the log file containing IP address access records.
    inst_ips (pd.DataFrame): DataFrame containing IP address ranges associated with a institution.

    Returns:
    pd.DataFrame: A DataFrame with three columns:
                  - `month`: The month of the recorded views.
                  - `day`: The day of the month when the views occurred.
                  - `usage`: The total number of views recorded for that institution on each day.
    """
    # Extract the IP networks for the institution
    ip_networks = inst_ips.iloc[0]['IPs']

    # Access log file
    log_df = logs_to_df(log_file)

    # some comment here 
    reqs_paths = ['{https|www.canadiana.ca}', '{https|gac.canadiana.ca}', '{https|parl.canadiana.ca}', '{https|heritage.canadiana.ca}']
    http_req_ptrn = '/view'

    # Filter log_df 
    filtered_log_df = filter_logs(log_df, reqs_paths, http_req_ptrn, ip_networks)
    print(filtered_log_df)
    filtered_log_df.to_csv("data/filtered_logs.csv", index=None)

    # Group by 'month' and 'day', and count views
    usage_df = filtered_log_df.groupby(['month', 'day']).size().reset_index(name='usage')

    # Define the order for months
    month_order = [
        'Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'
    ]

    # Convert 'month' to a categorical type with a specified order
    usage_df['month'] = pd.Categorical(usage_df['month'], categories=month_order, ordered=True)

    # Sort the DataFrame by 'month' and 'day'
    usage_df = usage_df.sort_values(by=['month', 'day'])

    # Reset index
    usage_df.reset_index(drop=True, inplace=True)

    return usage_df


def check_inst_name(inst_ips, inst_name):
    """
    Checks if the given institution name is present in the DataFrame.

    This function verifies if the DataFrame `inst_ips` is empty. If it is empty,
    it indicates that the institution name provided is not found. Otherwise, it
    confirms the presence of the institution.

    Parameters:
    inst_ips (pd.DataFrame): The DataFrame containing institution data.
    inst_name (str): The name of the institution to check.

    Returns:
    bool: Returns `True` if the institution is found (i.e., DataFrame is not empty),
          otherwise returns `False`.
    """
    if inst_ips.empty: 
        print(f"Error: Institution `{inst_name}` not found.")
        return False
    else:
        print(f"Institution '{inst_name}' found.")
        return True

def main():
    # Create the parser
    parser = argparse.ArgumentParser(description='Generate a usage report for a specified institution.')

    # Add arguments for the institution name and log file(s)
    parser.add_argument('institution', type=str, help='The institution for which to generate the usage report.')
    parser.add_argument('logs', type=str, help='The logs to process.')
    #parser.add_argument('logs', nargs='+', type=str, help='The logs to process.')

    # Parse the arguments
    args = parser.parse_args()

    #log_file = 'data/haproxy-traffic.log'
    #log_file = 'data/crkn-test.log'
    ip_file = 'data/IP_addresses.xlsx'
    skip_rows = 2
    #inst_name = 'Canadian Research Knowledge Network'

    # Load institutions from the IP addresses file
    ips_df = process_ip_file(ip_file, skip_rows)

    # Normalize both the DataFrame column and the argument for case-insensitive comparison
    inst_name_lower = args.institution.lower()
    ips_df['Institution_lower'] = ips_df['Institution'].str.lower()

    # Get row for institution name
    inst_ips = ips_df[ips_df['Institution_lower'] == inst_name_lower]
    
    if check_inst_name(inst_ips, args.institution):
        # Count the number of views for inst_name
        view_counts_df = count_views(args.logs, inst_ips)

        # Export view count to csv
        view_counts_df.to_csv("data/result.csv", index=None, header=["Month","Day","Usage"])
    
        print(view_counts_df)

if __name__ == '__main__':

    main()