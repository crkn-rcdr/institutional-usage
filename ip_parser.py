import pandas as pd
import re

def is_ip_in_range(ip, start, end):
    """
    Check if an IP address falls within a specified range.

    Args:
        ip (str): The IP address to check, in the format 'xxx.xxx.xxx.xxx'.
        start (str): The starting IP address of the range, in the same format.
        end (str): The ending IP address of the range, in the same format.

    Returns:
        bool: True if the IP address `ip` is within the range defined by `start` and `end`, False otherwise.
    """
    # Convert IP addresses from strings to lists of integers for comparison
    ip_parts = list(map(int, ip.split('.')))
    start_parts = list(map(int, start.split('.')))
    end_parts = list(map(int, end.split('.')))
    
    # Compare each octet of the IP address with the start and end ranges
    for i in range(4):  # Assuming IPv4 addresses
        if not (start_parts[i] <= ip_parts[i] <= end_parts[i]):
            return False
    
    return True

def is_ip_match(ip, ip_list):
    """
    Checks if an IP address matches any IP address in a list of IP addresses.

    Args:
        ip (str): A string representing a single IP address.
        ip_list (list): A list of IP addresses, IP ranges and/or IP patterns.

    Returns:
        bool: True if the IP address `ip` matches any item in the IP addresses list `ip_list`, False otherwise.
    """
    for item in ip_list:
        if isinstance(item, dict):
            if is_ip_in_range(ip, item['start'], item['end']):
                # print(f'{ip} found in {item}')
                return True
        elif isinstance(item, str):
            if ip == item:
                # print(f'{ip} found in {item}')
                return True
            
    return False

def parse_ip(ip_str):
    """
    Parse an IP address string and return either a single IP address or a dictionary
        with start and end IP addresses.

    Args:
        ip_str (str): A string representing a single IP address or a range of IP addresses.

    Returns:
        Union[str, dict]: 
            - If the input is a single IP address, returns the IP address as a string.
            - If the input is a range of IP addresses, returns a dictionary with 'start' and 'end' keys.
    """
    #print(ip_str)
    # Remove spaces
    ip_str = ip_str.strip()
    
    # Use regular expression to keep only numbers, ".", "*", and "-"
    ip_str = re.sub(r'[^0-9.*-]', '', ip_str)
    
    # Remove spaces and any trailing periods (just in case)
    ip_str = ip_str.strip()
    ip_str = ip_str.strip(".")
    
    # Regular expression pattern for exact start IP to exact end IP range
    ip_range_pattern = r'^(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})-(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})$'
    
    # Check if the string matches the IP range pattern
    if re.match(ip_range_pattern, ip_str):
        start_ip, end_ip = ip_str.split('-')
        # Make sure each part has no trailing zeros
        start_parts = [str(int(item)) for item in start_ip.split('.')]
        end_parts = [str(int(item)) for item in end_ip.split('.')]
        # Update start_ip and end_ip
        start_ip = '.'.join(start_parts)
        end_ip = '.'.join(end_parts)
    # Check for range within octets
    elif '-' in ip_str or '*' in ip_str: 
        parts = ip_str.split('.')
        start_parts = []
        end_parts = []
        for part in parts:
            if '-' in part:
                start, end = part.split('-')
                start_parts.append(str(int(start))) # making sure there are no trailing zeros
                end_parts.append(str(int(end)))
            elif part == '*':
                start_parts.append('0')
                end_parts.append('255')
            else:
                start_parts.append(str(int(part)))
                end_parts.append(str(int(part)))

        start_ip = '.'.join(start_parts)
        end_ip = '.'.join(end_parts)
    else: # i.e. single address
        # Make sure each part has no trailing zeros
        parts = [str(int(item)) for item in ip_str.split('.')]
        # Update ip_str
        ip_str = '.'.join(parts)
        #print(ip_str)
        return ip_str
    #print(f'start: {start_ip}\nend: {end_ip}')
    return {'start': start_ip, 'end': end_ip}

def process_ips(insts_df):
    """
    Processes a DataFrame of institutions to parse and clean IP addresses and Proxy Servers.

    Args:
        insts_df (pd.DataFrame): DataFrame containing institution data with columns 'IP Addresses' and 'Proxy Server'.
        
    Returns:
        pd.DataFrame: DataFrame with the 'IP Addresses' column containing parsed and cleaned IP addresses and Proxy Servers.
    """

    for idx, row in insts_df.iterrows():
        # Create empty list to store ips
        ip_list = []
        proxy_list = []
    
        if isinstance(row["IP Addresses"], str): # check if it is a non-null string
            ip_list = row["IP Addresses"]
            ip_list = [parse_ip(item) for item in ip_list.split("\n") if re.search(r'^\s*\d+[\d\.\-\*\s]*', item)]

        if isinstance(row["Proxy Server"], str) and not isinstance(row["Proxy Server"], list):
            proxy_list = row["Proxy Server"]
            proxy_list = [parse_ip(item) for item in proxy_list.split("\n") if re.search(r'^\s*\d+[\d\.\-\*\s]*', item)]

        insts_df.at[idx, "IP Addresses"] = ip_list + proxy_list

    # Drop Proxy Server column
    insts_df.drop(["Proxy Server"], axis=1, inplace=True)

    return insts_df

def ips_to_df(file_path, skip_rows):
    """
    Reads an Excel file containing institution data, processes the IP addresses, and returns a DataFrame.
    
    Args:
        file_path (str): Path to the Excel file containing the institution data.
        skip_rows (int): Number of rows to skip at the beginning of the file (e.g., header rows).
        
    Returns:
        pd.DataFrame or None: 
            - A DataFrame containing the institution names and their processed IP addresses if the file is successfully read.
            - Returns `None` if the file is not found or cannot be read.
    """ 
    try:
        df = pd.read_excel(file_path, skiprows=skip_rows)

        # Select institution name, IP Addresses and Email columns
        df = df[['Institution', 'IP Addresses', 'Proxy Server', 'Abbreviation']]

        # Remove rows where institution name is NaN 
        df = df.dropna(subset=['Institution']).reset_index(drop=True)

        # Deal with NaN values
        df[['Proxy Server', 'IP Addresses']] = df[['Proxy Server', 'IP Addresses']].fillna("")

        # Remove any leading and trailing spaces in each institution name
        df['Institution'] = df['Institution'].apply(lambda name: name.strip() if isinstance(name, str) else name)

        return df
    except FileNotFoundError:
        print(f"Error: The file at {file_path} was not found.")
        return None

    except pd.errors.EmptyDataError:
        print("Error: The file is empty.")
        return None

    except pd.errors.ParserError:
        print("Error: There was a problem parsing the file.")
        return None
        

def process_ip_file(file_path, skip_rows):
    """
    Reads an Excel file, processes the IP addresses, and returns a cleaned DataFrame.

    Args:
        file_path (str): Path to the Excel file containing the institution data.
        skip_rows (int): Number of rows to skip at the beginning of the file (e.g., header rows).

    Returns:
        pd.DataFrame or None: 
            - A DataFrame containing the institution names and their processed IP addresses.
            - Returns `None` if the file cannot be processed.
    """
    # Attempt to load the DataFrame
    insts_df = ips_to_df(file_path, skip_rows)

    # If loading was successful, process the IP addresses
    if insts_df is not None:
        return process_ips(insts_df)

    return None