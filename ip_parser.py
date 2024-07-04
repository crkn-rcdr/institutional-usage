import pandas as pd
import ipaddress
import re

def expand_range_pattern(pattern):
    """
    Converts an IP pattern into a list of IP addresses that fall within the specified range.

    Parameters:
    pattern (str): An IP pattern with ranges and wildcards.

    Returns:
    list: A list of ipaddress.IPv4Network objects representing individual IP addresses in /32 format.
    """
    parts = pattern.split('.')
    ranges = []

    # Identify and expand the range for each part
    for part in parts:
        if '-' in part:
            start, end = map(int, part.split('-'))
            ranges.append(range(start, end + 1))
        elif part == '*':
            ranges.append(range(256))  # Represents 0-255 for wildcard
        else:
            ranges.append(part)

    # Generate all combinations of IP addresses from the ranges
    networks = []
    for first_octet in ranges[0]:
        for second_octet in ranges[1]:
            for third_octet in ranges[2]:
                for fourth_octet in ranges[3]:
                    ip = f"{first_octet}.{second_octet}.{third_octet}.{fourth_octet}"
                    networks.append(ipaddress.ip_network(ip + '/32'))  # Single IP in /32 format
    return networks

def parse_ip(ip_str):
    """
    Parse an IP address string and return the corresponding IPv4 network

    This function handles two types of IP address inputs:
    1. A single IP address, which will be converted to an IPv4 network.
    2. A range of IP addresses specified using either '*' or '-' (or a combination of both).

    Args:
        ip_str (str): A string representing a single IP address or a range of IP addresses.

    Returns:
        ipaddress.IPv4Network or list: An IPv4 network object if a single IP address is provided, 
            or a list of IPv4 address ranges if a range pattern is detected.
    """
    # remove spaces
    ip_str = ip_str.strip()
    # Use regular expression to keep only numbers, ".", "*", and "-"
    ip_str = re.sub(r'[^0-9.*-]', '', ip_str)

    # Create a network for the IP address(es)
    if ip_str.startswith("IPv6"): 
        return ""
    elif "*" not in ip_str and "-" not in ip_str: # i.e. single address
        # Make sure each part has no trailling zeros
        parts = [str(int(item)) for item in ip_str.split('.')]
        # Update ip_str
        ip_str = '.'.join(parts)
        # Create network
        ip = ipaddress.IPv4Address(ip_str)
        network = ipaddress.IPv4Network(ip)
        return network
    elif '-' not in ip_str: # if no specific range
        parts = ip_str.split('.')
        expanded_parts = []
        num_wildcards = 0
        wildcard_found = False

        for part in parts:
            if part == '*':
                wildcard_found = True
                num_wildcards += 1
                expanded_parts.append('0')
            elif wildcard_found and part != '*':
                # If we find a non-wildcard part after a wildcard, use expand_range_pattern
                return expand_range_pattern(ip_str)
            else:
                expanded_parts.append(part)

        base_ip = '.'.join(expanded_parts)
        prefix_length = 32 - (num_wildcards * 8)
        return ipaddress.ip_network(base_ip + f'/{prefix_length}')
    else:
        return expand_range_pattern(ip_str)


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

        if isinstance(row["IP Addresses"], str): # check if it is a non-null string
            ip_list = row["IP Addresses"]
            ip_list = [item for item in ip_list.split("\n") if any(char.isdigit() for char in item)]
    

            for ip in ip_list:  
                #if pd.notnull(ip):
                network = parse_ip(ip)
                if isinstance(network, list):
                    lon.extend(network)
                else:
                    lon.append(network)

            # Flatten lon 
            lon = [network for network in lon if network and network != ""] if lon else []

        insts_df.at[idx, "IP Addresses"] = lon

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
    df = df[["Institution", "IP Addresses"]]

    # Remove rows where institution name is NaN 
    df = df.dropna(subset=["Institution"]).reset_index(drop=True)
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

def main(): 
    file_path = "data/IP_addresses.xlsx"
    skip_rows = 2
    ips_df = process_ip_file(file_path, skip_rows)

if __name__ == '__main__':
    main()