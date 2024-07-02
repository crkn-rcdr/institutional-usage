import pandas as pd

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
    df = df[df['Request'].str.startswith(reqs_ptrn)]
    
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
    # convert log file to pandas dataframe
    log_file = 'data/haproxy-traffic.log'
    log_df = logs_to_df(log_file)
    print(log_df.head())
    print(log_df.columns)

    # filter log_df 
    reqs_ptrn = 'GET https://www.canadiana.ca/view/'
    server_name = 'cap/local'
    filtered_log_df = filter_logs(log_df, reqs_ptrn=reqs_ptrn, server_name=server_name)
    print(filtered_log_df.head())
    
if __name__ == '__main__':
    main()