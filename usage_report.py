
from filter_logs import filter_ips
from ip_parser import process_ip_file
import pandas as pd 
import argparse
import os

def update_file(new_df, master_file_path, institution):
    # Create the directory if it doesn't exist
    os.makedirs(os.path.dirname(master_file_path), exist_ok=True)

    # Check if the file exists
    file_exists = os.path.isfile(master_file_path)

    if file_exists:
        try:
            # Try to read the existing sheet
            existing_df = pd.read_excel(master_file_path, sheet_name=institution)
            
            # # Merge existing data with new data
            # new_df = pd.concat([existing_df, new_df]).drop_duplicates(keep='last').reset_index(drop=True)

            # Merge existing data with new data
            combined_df = pd.concat([existing_df, new_df])
            
            # Convert all columns to string for consistent comparison
            combined_df = combined_df.astype(str)
            
            # Drop duplicates
            combined_df = combined_df.drop_duplicates(keep='last').reset_index(drop=True)
            
            # Convert numbers back to integers
            combined_df[1:] = combined_df[1:].astype(int)
            
            with pd.ExcelWriter(master_file_path, engine='openpyxl', mode='a', if_sheet_exists='replace') as writer:
                combined_df.to_excel(writer, sheet_name=institution, index=None)
        except ValueError:
            # Sheet doesn't exist, so we'll just use new_df as is
            pass
            
    with pd.ExcelWriter(master_file_path, engine='openpyxl', mode='w') as writer:
        new_df.to_excel(writer, sheet_name=institution, index=None)
    print(f"{'Updated' if file_exists else 'Created'} file: {master_file_path}, sheet: {institution}")

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
    log_df = pd.read_csv(log_file, index_col=False, on_bad_lines='warn')

    # Filter log_df 
    filtered = filter_ips(log_df, ip_networks)
    print(filtered)
    filtered.to_csv("data/filtered_logs.csv", index=None)

    # Group by month, day, and request_path, then count occurrences
    grouped = filtered.groupby(['month', 'day', 'request_path']).size().reset_index(name='count')

    # Pivot the result
    usage_df = grouped.pivot(index=['month', 'day'], columns='request_path', values='count').reset_index()

    # Fill NaN values with 0
    usage_df = usage_df.fillna(0).astype({col: int for col in usage_df.columns if col not in ['month']})
    
    # Define the order for months
    month_order = [
        'Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'
    ]
    
    # Convert 'month' to a categorical type with a specified order
    usage_df['month'] = pd.Categorical(usage_df['month'], categories=month_order, ordered=True)

    # Sort the DataFrame by 'month' and 'day'
    usage_df = usage_df.sort_values(by=['month', 'day'])

    # Reset index if needed
    usage_df = usage_df.reset_index(drop=True)

    # Define column names
    rename_dict = {'{https|www.canadiana.ca}': 'canadiana', 
                   '{https|gac.canadiana.ca}': 'gac', 
                   '{https|parl.canadiana.ca}': 'parl',
                   '{https|heritage.canadiana.ca}': 'heritage'}
    
    # Rename existing columns
    usage_df = usage_df.rename(columns=rename_dict)
    
    # Add missing columns with 0 values
    for old_col, new_col in rename_dict.items():
        if new_col not in usage_df.columns:
            usage_df[new_col] = 0
    
    # Calculate total usage
    usage_df['total'] = usage_df['heritage'] + usage_df['canadiana'] + usage_df['parl'] + usage_df['gac']
    
    # Reorder columns, including the new total_usage column
    column_order = ['month', 'day', 'heritage', 'canadiana', 'parl', 'gac', 'total']
    usage_df = usage_df[column_order]
    
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
    
    # Parse the arguments
    args = parser.parse_args()

    #log_file = 'data/haproxy-traffic.log'
    #log_file = 'data/crkn-test.log'
    ip_file = 'data/IP_addresses.xlsx'
    skip_rows = 2
    #inst_name = 'Canadian Research Knowledge Network'

    # Load institutions from the IP addresses file
    ips_df = process_ip_file(ip_file, skip_rows)

    # Get institution name 
    inst_name = args.institution
    
    # Get row for institution
    inst_ips = ips_df[(ips_df['Institution'].str.lower() == inst_name.lower())
                      | (ips_df['Domain'].str.lower() == inst_name.lower())]
    
    if check_inst_name(inst_ips, inst_name):
        # Count the number of views for inst_name
        view_counts_df = count_views(args.logs, inst_ips)
        
        file_name = "data/reports/usage-report.xlsx"
        
        #view_counts_df = append_to_master_csv(view_counts_df, file_name)
        # Export view count to csv
        #view_counts_df.to_csv(file_name, index=None)
        
        if len(inst_ips['Institution'].iloc[0]) > 30:
            inst_name = inst_ips['Domain'].iloc[0].upper()
        else:
            inst_name = inst_ips['Institution'].iloc[0]
            
        update_file(view_counts_df, file_name, inst_name)
        print(view_counts_df.to_string(index=False))

if __name__ == '__main__':
    main()