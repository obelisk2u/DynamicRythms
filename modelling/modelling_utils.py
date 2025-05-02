import pandas as pd
import glob
import numpy as np

def parse_outage():
    df = pd.read_csv('data\eaglei_data\eaglei_outages_2023.csv')
    df['run_start_time'] = pd.to_datetime(df['run_start_time'], errors='coerce')
    df['county'] = df['county'].str.upper()
    print(df.head())
    return df


def parse_storm():
    storms_df = pd.read_csv(r'data\NOAA_StormEvents\StormEvents_details-ftp_v1.0_d2023_c20241216.csv')
    storms_df['BEGIN_DATE_TIME'] = pd.to_datetime(storms_df['BEGIN_DATE_TIME'], errors='coerce') #make sure theyre datetime type
    storms_df['END_DATE_TIME'] = pd.to_datetime(storms_df['END_DATE_TIME'], errors='coerce')

    storms_df = storms_df[storms_df['BEGIN_DATE_TIME'] != storms_df['END_DATE_TIME']]  #get rid of storms that are recorded as 0 minutes

    storms_df['DURATION_MINUTES'] = (storms_df['END_DATE_TIME'] - storms_df['BEGIN_DATE_TIME']).dt.total_seconds() / 60   #get total duration of storm in minutes
    storms_df['storm_date'] = storms_df['BEGIN_DATE_TIME'].dt.date  # Extract date part (e.g., 2014-11-01)


    print(storms_df.columns.tolist())

    return storms_df


def merge(outages, storms):
    storms = storms[['EVENT_TYPE', 'CZ_NAME', 'BEGIN_DATE_TIME', 'END_DATE_TIME']]
    outages = outages[['customers_out', 'county', 'state', 'run_start_time']]

    merged_df = pd.merge(outages, storms, left_on='county', right_on='CZ_NAME', how='inner')

    filtered_df = merged_df[(merged_df['run_start_time'] >= merged_df['BEGIN_DATE_TIME']) & 
                            (merged_df['run_start_time'] <= merged_df['END_DATE_TIME'])]
    
    return filtered_df



def create_daily_aggregated_df(years):
    """
    Function to aggregate customers_out by day for each year and concatenate the results.
    
    Parameters:
    years (list): List of years to process (e.g., [2014, 2015, ..., 2023])
    
    Returns:
    pd.DataFrame: Concatenated DataFrame with daily aggregated customers_out for all years
    """
    # List to store each year's aggregated DataFrame
    dfs = []
    elec_path = r'data\eaglei_data\\'

    
    # Process each year
    for year in years:
        print(f"Processing year {year}...")
        
        # Step 1: Load the data for the year
        file_path = r'eaglei_outages_{year}.csv'
        try:
            df = pd.read_csv(elec_path+file_path, encoding="utf-8")
        except FileNotFoundError:
            print(f"File for year {year} not found. Skipping...")
            continue
        
        # Step 2: Convert run_start_time to datetime
        df['run_start_time'] = pd.to_datetime(df['run_start_time'])
        
        # Step 3: Set run_start_time as the index for resampling
        df.set_index('run_start_time', inplace=True)
        
        # Step 4: Resample by day and sum customers_out
        df_daily = df['customers_out'].resample('D').sum().reset_index()
        
        # Step 5: Add a 'year' column to identify the year
        df_daily['year'] = year
        
        # Append the resampled DataFrame to the list
        dfs.append(df_daily)
    
    # Step 6: Concatenate all DataFrames
    if not dfs:
        raise ValueError("No data was processed. Check if the files exist.")
    
    combined_df = pd.concat(dfs, ignore_index=True)
    print(combined_df.head())
    
    # Step 7: Rename columns for clarity
    combined_df.columns = ['date', 'customers_out', 'year']
    combined_df['date'] = pd.to_datetime(combined_df['date']).dt.date
    
    return combined_df
