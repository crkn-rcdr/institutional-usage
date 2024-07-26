import pandas as pd
from ip_parser import process_ip_file, is_ip_match
from dotenv import load_dotenv
import os

def filter_logs(df):
    """
    Filters out rows in logs DataFrame based on requests.

    Parameters:
    df (pd.DataFrame): Input DataFrame to be filtered.

    Returns:
    pd.DataFrame: Filtered DataFrame with matching rows.
    """
    # Load environment variables from .env file
    load_dotenv()

    server_name = os.getenv('SERVER_NAME')
    reqs_path = '{https|www.canadiana.ca}'
    http_req_ptrn = 'www.canadiana.ca/view/'

    # Drop rows with NaN values in 'http_request' column, if any
    df.dropna(subset=['http_request'], inplace=True)

    # Filter by server_name, request_path and http_request
    df = df[(df['server_name'] == server_name) 
            & ((df['request_path'].str.contains(reqs_path)) 
               | (df['http_request'].str.contains(http_req_ptrn)))
            & (df['http_request'].str.contains("view/"))]
    
    return df
    
def logs_to_df(file_path):
    """
    Converts a log file to a pandas DataFrame.

    Parameters:
    file_path (str): The path to the file to be read. 

    Returns:
    pd.DataFrame: A pandas DataFrame containing the data from the file.
                  It includes columns with date, IP address, server and HTTP request information.
    """
    # Define column names
    column_names = ["month", "day", "time", "hostname", "process_id", "client_ip_port", "accept_date", "frontend_name",
                    "server_name", "timers", "http_status_code", "bytes_read", "captured_request_cookie",
                    "captured_response_cookie", "termination_state", "retries", "connections_counts", "request_path", "http_request"]
    
    # Read log file
    log_df = pd.read_csv(file_path, sep=" ", header=None, names=column_names, index_col=False)

    # Split 'client_ip_port' to 'client_ip' and 'client_port'
    log_df[['client_ip', 'client_port']] = log_df['client_ip_port'].str.split(':', expand=True)

    # Drop the original combined column after splitting
    log_df = log_df.drop(columns=['client_ip_port'])

    # Select only relevant columns for analysis
    selected_columns = ["month", "day", "time", "client_ip", "server_name", "request_path", "http_request"]
    log_df = log_df[selected_columns].copy()

    return log_df

def process_logs(log_file):
    """
    Processes log data to count the number of accesses for each institution based on IP addresses.

    This function reads IP addresses from an Excel file and log data from a specified log file, 
    filters the logs, and counts the number of accesses for each institution based on the IP addresses 
    associated with them. The results are returned as a DataFrame with the institution names and 
    their corresponding view counts.

    Args:
        log_file (str): The path to the log file to be processed.

    Returns:
        pd.DataFrame: A DataFrame containing two columns:
                      - `institution`: The name of the institution.
                      - `view_count`: The number of times the institution was accessed.
    """
    # Convert IP addresses file to pandas DataFrame
    ip_file = 'data/IP_addresses.xlsx'
    # Specify the number of rows to skip at the beginning of the Excel file (e.g., header rows)
    skip_rows = 2
    ips_df = process_ip_file(ip_file, skip_rows)

    log_df = logs_to_df(log_file)

    # Filter log_df 
    filtered_log_df = filter_logs(log_df)

    # Count accesses for each institution
    view_counts = {}

    # Iterate through each institution and check accesses
    for institution_name, ip_networks in zip(ips_df['Institution'], ips_df['IPs']):
        mask = filtered_log_df['client_ip'].apply(lambda x: is_ip_match(x, ip_networks))
        view_counts[institution_name] = mask.sum()

    # Convert dictionary to Data Frame
    view_counts_df = pd.DataFrame(list(view_counts.items()), columns=['institution', 'view_count'])

    return view_counts_df

def count_views(log_file, ips_df, inst_name):
    """
    Counts the number of views for a specific institution based on IP logs.

    This function reads an Excel file containing IP addresses associated with institutions, 
    filters log data based on IP addresses, and counts the number of accesses for the 
    specified institution.

    Parameters:
    log_file (str): Path to the log file containing IP address access records.
    inst_name (str): The name of the institution to count views for.

    Returns:
    pd.DataFrame: A DataFrame with two columns: `institution` and `views_count`. 
                  It contains the institution name and the total number of views 
                  recorded for that institution.
    """
    # Get row for inst_name
    # add case that institution not found ask again or provide options idk
    inst_ips = ips_df[ips_df['Institution'] == inst_name]
    
    # Extract the IP networks for the institution
    ip_networks = inst_ips.iloc[0]['IPs']

    # Access log file
    log_df = logs_to_df(log_file)

    # Filter log_df 
    filtered_log_df = filter_logs(log_df)

    # Count accesses for the specific institution
    mask = filtered_log_df['client_ip'].apply(lambda x: is_ip_match(x, ip_networks))
    views_count = mask.sum()

    # Create DataFrame for the view counts
    view_counts_df = pd.DataFrame({'institution': [inst_name], 'views_count': [views_count]})

    return view_counts_df


def main():
    log_file = 'data/haproxy-traffic.log'
    ip_file = 'data/IP_addresses.xlsx'
    skip_rows = 2
    inst_name = 'Canadian Research Knowledge Network'

    # Load institutions from the IP addresses file
    ips_df = process_ip_file(ip_file, skip_rows)
    
    # Count the number of views for inst_name
    view_counts_df = count_views(log_file, ips_df, inst_name)

    # Uncomment to export view count to csv
    # view_counts_df.to_csv("data/result.csv", index=None)
    
    print(view_counts_df)

if __name__ == '__main__':
    main()