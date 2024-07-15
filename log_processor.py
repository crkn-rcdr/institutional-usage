import pandas as pd
from ip_parser import process_ip_file, is_ip_match

def filter_logs(df, reqs_ptrn, server_name):
    """
    Filters out rows in logs DataFrame based on requests and servers.   
    
    Parameters:
    df (pd.DataFrame): Input DataFrame to be filtered.
    reqs_ptrn (str): Request pattern to filter for.
    server_name (str): Server name to filter for.
    
    Returns:
    pd.DataFrame: Filtered DataFrame with matching rows.
    """
    # Drop rows with NaN values in 'Request' column, if any
    df.dropna(subset=['Request'], inplace=True)
    
    # Filter requests
    df = df[df['Request'].str.contains(reqs_ptrn)]
    
    # Filter server
    df = df[df['Server'] == server_name]
    
    return df

def logs_to_df(file_path):
    """
    Converts a log file to a pandas DataFrame.

    Parameters:
    file_path (str): The path to the file to be read. 

    Returns:
    pd.DataFrame: A pandas DataFrame containing the data from the file.
                  It includes columns with date, IP address, port, server and request information.
    """
    # Read file 
    log = pd.read_csv(file_path,sep=" ")
    column_names = ['Month', 'Day', 'Time', 'IP Address:Port', 'Server', 'Request']

    # Select the relevant columns, create a new DataFrame and rename columns
    log_df = log[[log.columns[0], log.columns[1], log.columns[2], log.columns[5], log.columns[8], log.columns[18]]].copy()
    log_df.columns = column_names

    # Split IP Address and Port into two separate columns
    log_df[['IP Address', 'Port']] = log_df['IP Address:Port'].str.split(':', expand=True)
    
    # Drop the 'IP Address:Port' column
    log_df.drop(columns=['IP Address:Port'], inplace=True)

    return log_df

def main():
    # Convert IP addresses file to pandas DataFrame
    ip_file = 'data/IP_addresses.xlsx'
    # Specify the number of rows to skip at the beginning of the Excel file (e.g., header rows)
    skip_rows = 2
    ips_df = process_ip_file(ip_file, skip_rows)
    print(ips_df)
    # Uncomment to export institutions to csv
    #ips_df.to_csv('data/insts_df.csv', index=False)

    # Convert log file to pandas DataFrame
    #log_file = 'data/haproxy-traffic.log'
    log_file = 'data/test-traffic.log'
    log_df = logs_to_df(log_file)

    # Filter log_df 
    reqs_ptrn = 'www.canadiana.ca/view/'
    server_name = 'cap/local'
    filtered_log_df = filter_logs(log_df, reqs_ptrn=reqs_ptrn, server_name=server_name)
    print(filtered_log_df.head())
    filtered_log_df.to_csv('data/filtered_logs.csv', index=False)

    # Count accesses for each institution
    inst_view_counts = {}

    # Iterate through each institution and check accesses
    for institution_name, ip_networks in zip(ips_df['Institution'], ips_df['IPs']):
        #print(f'Institution: {institution_name}')
        mask = filtered_log_df['IP Address'].apply(lambda x: is_ip_match(x, ip_networks))
        #print(f'Access Count: {mask.sum()}')
        inst_view_counts[institution_name] = mask.sum()

    # Output view counts
    # print("Institution Access Counts:\n")
    # for institution, count in inst_view_counts.items():
    #     print(f"Institution: {institution}, Access Count: {count}")
    
    # inst_views_df = pd.DataFrame(list(inst_view_counts.items()), columns=['Institution', 'Views'], index=None)
    # inst_views_df.to_csv("data/result.csv", index=None)


if __name__ == '__main__':
    main()