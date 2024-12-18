from filter_logs import * 
import argparse
from datetime import date
import os
import pandas as pd

def logs_parser(folder_path):
    """
    Parses log files from the specified folder and returns a combined Pandas DataFrame.

    Args:
        folder_path (str): The path to the folder containing the log files.

    Returns:
        pd.DataFrame: A Pandas DataFrame containing the filtered and combined log data.
    """
    # Get a list of all files in the folder
    file_names = [f for f in os.listdir(folder_path) if os.path.isfile(os.path.join(folder_path, f))]

    # Define the patterns to filter the log data
    reqs_paths = ['{https|www.canadiana.ca}', '{https|gac.canadiana.ca}', '{https|parl.canadiana.ca}', '{https|heritage.canadiana.ca}']
    http_req_ptrn = '/view'
        
    logs_list = []

    for file_name in file_names:
        file_name = os.path.join(folder_path, file_name)
        df = logs_to_df(file_name)
        
        filtered = filter_logs(df, reqs_paths, http_req_ptrn)
        
        logs_list.append(filtered)

    # Combine the filtered log data into a single DataFrame
    combined_df = pd.concat(logs_list, ignore_index=True)
    
    return combined_df

def main():
    # Create the parser
    parser = argparse.ArgumentParser(description='Clean log files from a specified folder.')

    # Add arguments for the institution name and log file(s)
    parser.add_argument('folder', type=str, help='The folder with the logs files to process')
    
    # Parse the arguments
    args = parser.parse_args()
    
    logs = logs_parser(args.folder)
    
    # Get the current date
    today = date.today()
    
    # Create the output directory if it doesn't exist
    output_dir = os.path.join("data", "processed")
    os.makedirs(output_dir, exist_ok=True)
    
    # Create the file path with the current date
    file_path = os.path.join(output_dir, f"logs_{today.strftime('%Y-%m-%d')}.csv")

    # Save the DataFrame to the CSV file
    logs.to_csv(file_path, index=None)
    
if __name__ == '__main__':
    main()