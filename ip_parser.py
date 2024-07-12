import pandas as pd
from ipaddress import ip_address, ip_network, summarize_address_range
import re

def expand_ip_range(start_ip, end_ip):
    """
    Expands a range of IP addresses into a list of contiguous IPv4 networks.

    Parameters:
    start_ip (str): Starting IP address of the range.
    end_ip (str): Ending IP address of the range.

    Returns:
    list: A list of ipaddress.IPv4Network objects representing the range.
    """
    start_ip = ip_address(start_ip)
    end_ip = ip_address(end_ip)
    return list(summarize_address_range(start_ip, end_ip))

def parse_ip(ip_str):
    """
    Parse an IP address string and return the corresponding IPv4 network

    This function handles three types of IP address inputs:
    1. A single IP address, which will be converted to an IPv4 network.
    2. A range of IP addresses specified using '*' or '-' (or a combination of both).
    3. A range of IP addresses specified with two IP addresses separated by '-'.

    Args:
        ip_str (str): A string representing a single IP address or a range of IP addresses.

    Returns:
        list: A list of ipaddress.IPv4Network objects representing the range or single address.
    """
    print(ip_str)
    # Remove spaces
    ip_str = ip_str.strip()

    # Ignore repeated addresses
    if ip_str.startswith("IPv6") or ip_str.startswith("IPv4"): 
        return ""
    
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
        return expand_ip_range(start_ip, end_ip)
    # Check for range within octets
    elif '-' in ip_str or '*' in ip_str:
        parts = ip_str.split('.')
        start_parts = []
        end_parts = []
        print(start_parts)
        for part in parts:
            print(part)
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
        print("after")
        print(start_parts)
        print(end_parts)

        start_ip = '.'.join(start_parts)
        end_ip = '.'.join(end_parts)
        print("start_ip:")
        print(start_ip)
        return expand_ip_range(start_ip, end_ip)
    else: # i.e. single address
        # Make sure each part has no trailing zeros
        parts = [str(int(item)) for item in ip_str.split('.')]
        # Update ip_str
        ip_str = '.'.join(parts)
        # Create network
        ip = ip_address(ip_str)
        network = ip_network(ip)
        return [network]


def process_ips(insts_df):
    """
    Processes a DataFrame of institutions to parse and clean IP addresses.

    Args:
        insts_df (pd.DataFrame): DataFrame containing institution data with a column 'IP Address'.
        
    Returns:
        pd.DataFrame: DataFrame with the 'IP Address' column containing parsed and cleaned IP addresses.
    """

    for idx, row in insts_df.iterrows():
        # Create empty list to store networks
        lon = []

        if isinstance(row["IPs"], str): # check if it is a non-null string
            ip_list = row["IPs"]
            ip_list = [item for item in ip_list.split("\n") if any(char.isdigit() for char in item)]
    

            for ip in ip_list:  
                network = parse_ip(ip)
                if isinstance(network, list):
                    lon.extend(network)
                else:
                    lon.append(network)

            # Flatten lon 
            lon = [network for sublist in lon for network in sublist if network != ""] if lon else []

        insts_df.at[idx, "IPs"] = lon

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
    df = df.dropna(subset=['Institution']).reset_index(drop=True)

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

def is_ip_in_networks(ip_str, networks_list):
    """
    Check if an IP address is in any network in a list.

    Args:
        ip_str (str): The IP address to check.
        networks_list (list): A list of IPv4Network objects to check against.

    Returns:
        bool: True if the IP is in any of the networks, False otherwise.
    """
    ip = ip_address(ip_str)
    return any(ip in net for net in networks_list)