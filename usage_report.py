
from filter_logs import filter_ips
from ip_parser import process_ip_file
import pandas as pd 
import argparse
import os

def sort_and_filter(df):
    """
    Processes and sorts a DataFrame by month and day, while handling missing values and removing duplicates.

    Parameters:
    df (pd.DataFrame): The DataFrame to process.

    Returns:
    pd.DataFrame: A DataFrame that is sorted by 'Month', 'Day', and 'Total', with duplicates removed and missing values handled.
    """
    int_columns = ['Day', 'Heritage', 'Canadiana', 'GAC', 'Parl', 'Total']  
    
    # Fill missing values in the specified columns with 0 and convert the columns to integer type 
    df[int_columns] = df[int_columns].fillna(0).astype(int)
    
    # Define the order for months
    month_order = [
        'Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'
    ]
    
    # Convert 'month' to a categorical type with a specified order
    df['Month'] = pd.Categorical(df['Month'], categories=month_order, ordered=True)

    # Sort the DataFrame by 'month', 'day' and 'total' 
    df = df.sort_values(by=['Month', 'Day', 'Total'])
    
    # Drop duplicates, keep the one with most views
    df = df.drop_duplicates(['Month', 'Day'], keep='last')
    
    return df
    
def update_file(new_df, master_file_path, institution):
    """
    Updates an Excel file with new data from a DataFrame. If the specified sheet 
    exists in the file, the function appends the new data to it, updates the existing 
    data, and saves it. If the sheet does not exist, it creates a new sheet with the 
    provided data. If the file does not exist, it creates a new file and adds the sheet.

    Parameters:
    new_df : pd.DataFrame
        The DataFrame containing new data to be added to the Excel file. The DataFrame 
        should have a column named 'month' that will be sorted according the year calendar order
        and should include columns 'day' and 'total'.

    master_file_path : str
        The path to the Excel file to be updated. This includes the filename and should 
        end with '.xlsx'.

    institution : str
        The name of the sheet in the Excel file where data should be updated or created.

    Returns:
    None
        This function does not return any value. It updates or creates an Excel file and 
        sheet as specified
    """
    os.makedirs(os.path.dirname(master_file_path), exist_ok=True)
    file_exists = os.path.isfile(master_file_path)
    print(f"File exists: {file_exists}")

    if file_exists:
        try:
            existing_df = pd.read_excel(master_file_path, sheet_name=institution)
            print(f"Successfully read existing sheet: {institution}")

            combined_df = pd.concat([existing_df, new_df], ignore_index=True)

            combined_df = sort_and_filter(combined_df)
            
            with pd.ExcelWriter(master_file_path, engine='openpyxl', mode='a', if_sheet_exists='replace') as writer:
                combined_df.to_excel(writer, sheet_name=institution, index=None)
            print(f"Updated existing sheet: {institution}")
        except ValueError:
            print(f"Sheet {institution} does not exist. Creating new sheet.")
            new_df = sort_and_filter(new_df)
            with pd.ExcelWriter(master_file_path, engine='openpyxl', mode='a', if_sheet_exists='replace') as writer:
                new_df.to_excel(writer, sheet_name=institution, index=None)
    else:
        print(f"File does not exist. Creating new file and sheet.")
        new_df = sort_and_filter(new_df) 
        with pd.ExcelWriter(master_file_path, engine='openpyxl', mode='w') as writer:
            new_df.to_excel(writer, sheet_name=institution, index=None)

    print(f"{'Updated' if file_exists else 'Created'} file: {master_file_path}, sheet: {institution}")
    
def count_views(log_file, inst_ips):
    """
    Counts the number of views for a specific institution based on logs.

    This function processes a log file to count the number of accesses for an institution by filtering log entries 
    based on associated IP addresses. The counts are aggregated by month and day, and the results are presented 
    in a DataFrame with columns for each request path and a total count of views.

    Parameters:
    log_file : str
        Path to the CSV log file containing IP address access records. The file is read into a DataFrame for processing.

    inst_ips : pd.DataFrame
        DataFrame containing IP address ranges associated with the institution. The first row's 'IPs' column is used 
        to filter the log data.

    Returns:
    pd.DataFrame
        A DataFrame with the following columns:
        - `Month`: The month of the recorded views.
        - `Day`: The day of the month when the views occurred.
        - `Heritage`: Number of views for the 'heritage' request path.
        - `Canadiana`: Number of views for the 'canadiana' request path.
        - `Parl`: Number of views for the 'parl' request path.
        - `GAC`: Number of views for the 'gac' request path.
        - `Total`: Total number of views across all request paths for each day.
    """
    # Extract the IP networks for the institution
    ip_networks = inst_ips.iloc[0]['IPs']

    # Access log file
    log_df = pd.read_csv(log_file, index_col=False, on_bad_lines='skip')

    # Filter log_df 
    filtered = filter_ips(log_df, ip_networks)
    print(filtered)

    # Group by month, day, and request_path, then count occurrences
    grouped = filtered.groupby(['month', 'day', 'request_path']).size().reset_index(name='count')

    # Pivot the result
    usage_df = grouped.pivot(index=['month', 'day'], columns='request_path', values='count').reset_index()

    # Fill NaN values with 0
    usage_df = usage_df.fillna(0).astype({col: int for col in usage_df.columns if col not in ['month']})

    # Define column names
    rename_dict = {'month': 'Month',
                   'day': 'Day',
                   '{https|www.canadiana.ca}': 'Canadiana', 
                   '{https|gac.canadiana.ca}': 'GAC', 
                   '{https|parl.canadiana.ca}': 'Parl',
                   '{https|heritage.canadiana.ca}': 'Heritage'}
    
    # Rename existing columns
    usage_df = usage_df.rename(columns=rename_dict)
    
    # Add missing columns with 0 values
    for new_col in rename_dict.items():
        if new_col not in usage_df.columns:
            usage_df[new_col] = 0
    
    # Calculate total usage
    usage_df['Total'] = usage_df['Heritage'] + usage_df['Canadiana'] + usage_df['Parl'] + usage_df['GAC']
    
    # Reorder columns, including the new total_usage column
    column_order = ['Month', 'Day', 'Heritage', 'Canadiana', 'GAC', 'Parl', 'Total']
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

    ip_file = 'data/IP_addresses.xlsx'
    skip_rows = 2

    print("Loading IP Addresses file...")
    # Load institutions from the IP addresses file
    ips_df = process_ip_file(ip_file, skip_rows)

    # Check if the DataFrame was loaded successfully
    if ips_df is None:
        print("Error loading IP addresses file. Exiting program.")
        return 
    
    # Get institution name 
    inst_name = args.institution

    # Get row for institution
    inst_ips = ips_df[(ips_df['Institution'].str.lower() == inst_name.lower())
                    | (ips_df['Abbreviation'].str.lower() == inst_name.lower())]
    
    if check_inst_name(inst_ips, inst_name):
        # Count the number of views for inst_name
        view_counts_df = count_views(args.logs, inst_ips)
        
        file_name = "data/reports/usage-report.xlsx"
        
        # Use institution name abbreviation if it exceeds 30 characters (Excel sheet naming limit)
        if len(inst_ips['Institution'].iloc[0]) > 30:
            inst_name = inst_ips['Abbreviation'].iloc[0]
        else:
            inst_name = inst_ips['Institution'].iloc[0]
        
        print(view_counts_df.to_string(index=False))
        
        # Update (or create new) institution sheet in usage reports file
        update_file(view_counts_df, file_name, inst_name)
        
        print()

        
if __name__ == '__main__':
    main()