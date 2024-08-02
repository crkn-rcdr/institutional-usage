import pandas as pd
from ip_parser import is_ip_match
#from dotenv import load_dotenv
#import os
import re

def filter_logs(df, reqs_paths, http_req_ptrn, ip_networks):
    """
    Filters out rows in logs DataFrame based on requests.
 
    Parameters:
    df (pd.DataFrame): Input DataFrame to be filtered.

    Returns:
    pd.DataFrame: Filtered DataFrame with matching rows.
    """
    # Load environment variables from .env file
    #load_dotenv()

    #server_name = os.getenv('SERVER_NAME')

    # Join the items in reqs_path with a pipe symbol
    reqs_paths = '|'.join(re.escape(item) for item in reqs_paths)

    # Drop rows with NaN values in 'http_request' column, if any
    df.dropna(subset=['http_request'], inplace=True)

    # Filter by request_path, http_request and ip
    df = df[((df['request_path'].str.contains(reqs_paths)) 
               | (df['http_request'].str.contains(http_req_ptrn)))
            & (df['http_request'].str.contains("view/"))
            & (df.apply(lambda row: is_ip_match(row['client_ip'], ip_networks), axis=1))]
    
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
