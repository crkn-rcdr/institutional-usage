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
                return True
        elif isinstance(item, str):
            if ip == item:
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
    print(ip_str)
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
    Processes a DataFrame of institutions to parse and clean IP addresses.

    Args:
        insts_df (pd.DataFrame): DataFrame containing institution data with a column 'IP Address'.
        
    Returns:
        pd.DataFrame: DataFrame with the 'IP Address' column containing parsed and cleaned IP addresses.
    """

    for idx, row in insts_df.iterrows():
        # Create empty list to store ips
        ip_list = []

        if isinstance(row["IPs"], str): # check if it is a non-null string
            ip_list = row["IPs"]
            ip_list = [parse_ip(item) for item in ip_list.split("\n") if any(char.isdigit() for char in item)]

        insts_df.at[idx, "IPs"] = ip_list

    return insts_df

def ips_to_df(file_path, skip_rows):
    """
    Reads an Excel file containing institution data, processes the IP addresses, and returns a DataFrame.
    
    Args:
        file_path (str): Path to the Excel file containing the institution data.
        skip_rows (int): Number of rows to skip at the beginning of the file (e.g., header rows).
        
    Returns:
        pd.DataFrame: DataFrame containing the institution names and their processed IP addresses.
    """
    df = pd.read_excel(file_path, skiprows=skip_rows)

    # Select institution name and IP Addresses column
    # Later will have to accomodate to proxy as well
    df = df[['Institution', 'IP Addresses']]

    # Rename IP column
    df = df.rename(columns={'IP Addresses': 'IPs'})

    # Remove rows where institution name is NaN 
    df = df.dropna(subset=['Institution', 'IPs']).reset_index(drop=True)

    return df

def process_ip_file(file_path, skip_rows):
    """
    Reads an Excel file, processes the IP addresses, and returns a cleaned DataFrame.

    Args:
        file_path (str): Path to the Excel file containing the institution data.
        skip_rows (int): Number of rows to skip at the beginning of the file (e.g., header rows).

    Returns:
        pd.DataFrame: DataFrame containing the institution names and their processed IP addresses.
    """
    insts_df = ips_to_df(file_path, skip_rows)
    ips_df = process_ips(insts_df)
    return ips_df