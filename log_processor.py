import pandas as pd
from ip_parser import process_ip_file, is_ip_match

def filter_logs(df):
    """
    Filters out rows in logs DataFrame based on requests.

    Parameters:
    df (pd.DataFrame): Input DataFrame to be filtered.

    Returns:
    pd.DataFrame: Filtered DataFrame with matching rows.
    """
    server_name = 'cap/local'
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

def main():
    # Convert IP addresses file to pandas DataFrame
    ip_file = 'data/IP_addresses.xlsx'
    # Specify the number of rows to skip at the beginning of the Excel file (e.g., header rows)
    skip_rows = 2
    ips_df = process_ip_file(ip_file, skip_rows)

    # Convert log file to pandas DataFrame
    #log_file = 'data/haproxy-traffic.log'
    log_file = 'data/test-traffic.log'
    log_df = logs_to_df(log_file)

    # Filter log_df 
    filtered_log_df = filter_logs(log_df)
    print(filtered_log_df.head())
    filtered_log_df.to_csv('data/filtered_logs.csv', index=False)

    # Count accesses for each institution
    inst_view_counts = {}

    # Iterate through each institution and check accesses
    for institution_name, ip_networks in zip(ips_df['Institution'], ips_df['IPs']):
        mask = filtered_log_df['client_ip'].apply(lambda x: is_ip_match(x, ip_networks))
        inst_view_counts[institution_name] = mask.sum()

    # Output view counts
    # print("Institution Access Counts:\n")
    # for institution, count in inst_view_counts.items():
    #     print(f"Institution: {institution}, Access Count: {count}")
    
    # Convert dictionary to Data Frame
    inst_views_df = pd.DataFrame(list(inst_view_counts.items()), columns=['Institution', 'Views'])
    # UNcomment to xport view count to csv
    # inst_views_df.to_csv("data/result.csv", index=None)


if __name__ == '__main__':
    main()