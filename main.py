import os
import csv
from multiprocessing import Pool, cpu_count
from datetime import datetime, timedelta

# Data Loading Functions
def find_csv_files(directory):
    # yelds every CSV files in the directory
    for root, dirs, files in os.walk(directory):
        for file in files:
            if file.startswith("ctg_tick_") and file.endswith(".csv"):
                yield os.path.join(root, file)

def read_tick_data(file_path):
    # Read rows from the CSV file
    with open(file_path, 'r') as file:
        csv_reader = csv.reader(file)
        next(csv_reader)
        for row in csv_reader:
            yield row

def process_file(file_path):
    # process the given files to get its rows, validate, and return the row, its validations, and errors (if any)
    unique_timestamps = set()
    results = []
    for row in read_tick_data(file_path):
        cleaned_row, errors = clean_and_validate_row(row, unique_timestamps)
        results.append((cleaned_row, errors, row))
    return results


# Data Cleaning & Validation
def clean_and_validate_row(row, unique_timestamps):
    errors = []

    # general validation to make sure each row has enough data
    if len(row) != 3:
        errors.append("Incorrect number of columns")
        return None, errors

    timestamp, price, size = row

    # timestamp validation
    # check if timestamp exists in row
    if not timestamp:
        errors.append("Missing timestamp")
        return None, errors

    try:
        # checks if timestamp format matches requirements
        datetime.strptime(timestamp, "%Y-%m-%d %H:%M:%S.%f")
        # check for duplicate timestamps
        if timestamp in unique_timestamps:
            errors.append("Duplicate timestamp")
            return None, errors
        unique_timestamps.add(timestamp)
    except ValueError:
        errors.append("Invalid timestamp format")
        return None, errors

    # Validate and clean price
    try:
        price = float(price)
        # make sure price is positive
        if price <= 0:
            errors.append("Price must be positive")
            return None, errors
    except ValueError:
        # if price is not entered
        errors.append("Invalid price value")
        return None, errors

    # check if price is too low (assumption)
    if price < 100:
        errors.append("Price too low (below 100)")
        return None, errors

    # general validation to make sure size is right
    try:
        size = int(size)
        if size < 0:
            errors.append("Size must be non-negative")
            return None, errors
    except ValueError:
        errors.append("Invalid size value")
        return None, errors

    return (timestamp, price, size), errors

# Data Transformation Functions
def parse_interval(interval_str):
    # custom function to be able to parse general interval inputs
    days, hours, minutes, seconds = 0, 0, 0, 0
    if 'd' in interval_str:
        parts = interval_str.split('d')
        days = int(parts[0])
        interval_str = parts[1] if len(parts) > 1 else ''
    if 'h' in interval_str:
        parts = interval_str.split('h')
        hours = int(parts[0])
        interval_str = parts[1] if len(parts) > 1 else ''
    if 'm' in interval_str:
        parts = interval_str.split('m')
        minutes = int(parts[0])
        interval_str = parts[1] if len(parts) > 1 else ''
    if 's' in interval_str:
        parts = interval_str.split('s')
        seconds = int(parts[0])
    return timedelta(days=days, hours=hours, minutes=minutes, seconds=seconds)

def aggregate_ohlcv(data, interval, start_time, end_time):
    ohlcv_data = {}
    interval_start = None
    current_ohlcv = {'open': None, 'high': float(
        '-inf'), 'low': float('inf'), 'close': None, 'volume': 0}
    
    # sort provided clean data by timestamp
    data.sort(key=lambda x: x[0])

    for timestamp, price, size in data:
        timestamp = datetime.strptime(timestamp, "%Y-%m-%d %H:%M:%S.%f")

        if timestamp < start_time or timestamp > end_time:
            continue

        if interval_start is None:
            interval_start = timestamp

        if timestamp >= interval_start + interval:
            # Save the current OHLCV data for the completed interval
            ohlcv_data[interval_start] = current_ohlcv
            # Start a new interval
            interval_start = timestamp
            current_ohlcv = {'open': None, 'high': float(
                '-inf'), 'low': float('inf'), 'close': None, 'volume': 0}

        if current_ohlcv['open'] is None:
            current_ohlcv['open'] = price
        current_ohlcv['high'] = max(current_ohlcv['high'], price)
        current_ohlcv['low'] = min(current_ohlcv['low'], price)
        current_ohlcv['close'] = price
        current_ohlcv['volume'] += size

    # to debug start and end timestamps
    # with open("./timestamp.csv", 'w', newline='') as file:
    #     for timestamp, price, size in data:
    #         timestamp = datetime.strptime(timestamp, "%Y-%m-%d %H:%M:%S.%f")

    #         if timestamp < start_time or timestamp > end_time:
    #             continue
    #         writer = csv.writer(file)

    #         # Write the header row
    #         writer.writerow([timestamp])

    #         if interval_start is None:
    #             interval_start = timestamp

    #         if timestamp >= interval_start + interval:
    #             # Save the current OHLCV data for the completed interval
    #             ohlcv_data[interval_start] = current_ohlcv
    #             # Start a new interval
    #             interval_start = timestamp
    #             current_ohlcv = {'open': None, 'high': float(
    #                 '-inf'), 'low': float('inf'), 'close': None, 'volume': 0}

    #         if current_ohlcv['open'] is None:
    #             current_ohlcv['open'] = price
    #         current_ohlcv['high'] = max(current_ohlcv['high'], price)
    #         current_ohlcv['low'] = min(current_ohlcv['low'], price)
    #         current_ohlcv['close'] = price
    #         current_ohlcv['volume'] += size

    # Save the last interval's OHLCV data
    if current_ohlcv['open'] is not None:
        ohlcv_data[interval_start] = current_ohlcv

    return ohlcv_data

def ohlcv_to_csv(ohlcv_data, output_file):
    with open(output_file, 'w', newline='') as csvfile:
        csv_writer = csv.writer(csvfile)
        csv_writer.writerow(
            ['Interval Start', 'Open', 'High', 'Low', 'Close', 'Volume'])
        for interval, data in ohlcv_data.items():
            csv_writer.writerow(
                [interval, data['open'], data['high'], data['low'], data['close'], data['volume']])

# User Input and Validation Functions
def get_interval_input():
    while True:
        interval_str = input(
            "\nEnter the time interval (e.g., '4s', '15m', '2h', '1d'): ").strip().lower()
        try:
            interval = parse_interval(interval_str)
            # interval validation. if not parseable, then returned time delta is equivalent to timedelta(0) (00:00:00)
            if interval == timedelta(0):
                raise ValueError

            # print(interval)
            return interval
        except ValueError:
            print("Invalid interval format. Please try again.")

def get_datetime_input(prompt):
    while True:
        datetime_str = input(
            f"{prompt} (format: 'YYYY-MM-DD HH:MM:SS'): ").strip()
        try:
            return datetime.strptime(datetime_str, "%Y-%m-%d %H:%M:%S")
        except ValueError:
            print("Invalid datetime format. Please try again.")


# Main Function
def main():
    csv_files = list(find_csv_files("./data"))

    # using multithreading to be able to load and read through csv files more efficiently
    with Pool(processes=cpu_count()) as pool:
        all_results = pool.map(process_file, csv_files)

    total_rows = 0
    valid_rows = 0
    invalid_rows = 0
    error_log = []

    cleaned_data = []

    # go through all data and return clean data and generate error log
    for file_results in all_results:
        for cleaned_row, errors, original_row in file_results:
            total_rows += 1
            if cleaned_row:
                valid_rows += 1
                cleaned_data.append(cleaned_row)
            else:
                invalid_rows += 1
                error_log.append((original_row, errors))

    print(f"\nTotal rows processed: {total_rows}")
    print(f"Valid rows after cleaning: {valid_rows}")
    print(f"Rows removed: {invalid_rows}")

    # Log errors if necessary
    # if error_log:
    #     print("\nSample Errors:")
    #     for i, (row, errors) in enumerate(error_log[:5]):
    #         print(f"Invalid row {i + 1}: {row}")
    #         print(f"Errors: {', '.join(errors)}")

    # user input functions for data interface
    interval = get_interval_input()
    start_time = get_datetime_input("Enter the start time")
    end_time = get_datetime_input("Enter the end time")

    # Check that start time is before end time
    if start_time >= end_time:
        print("Start time must be before end time.")
        return

    # Aggregate data into OHLCV bars
    ohlcv_data = aggregate_ohlcv(cleaned_data, interval, start_time, end_time)

    # Write OHLCV data to CSV
    output_file = "./ohlcv_output.csv"
    ohlcv_to_csv(ohlcv_data, output_file)
    print(f"\nOHLCV data written to {output_file}")


if __name__ == "__main__":
    main()

# 1. Data Loading Documentation
    # Approach
        # Collect all csv files and process every file's rows
        # Maintained validation for file collection by using the naming standards/conventions
    # Challenges
        # Originally slow - dropped time by more than half (~19 sec to ~7 sec) by using Python multiprocessing

# 2. Data Cleaning
    # Approaches & Assumptions
        # Implement broadest data validation (number of columns, missing values, size is non-negative)
        # Implement the 4 consistent errors:
            # 1. Duplicate timestamps - Seems unlikely that there are orders at the EXACT same time down to the milliseconds
            # 2. Negative Prices - Make sure price is positive(negative prices don't make sense)
            # 3. Abnormally Low Prices - Disregard where prices are below 100 because it seems that the decimal placement is inaccurate
            # 4. Missing Price Values - Disregard where there are no price values provided

# 3. Data Interface
    # Approach
        # 1. Get user input for start, end, and interval
        # 2. Validate start and end times
        # 3. Aggregate all rows with data entries between the start and end with each entry within the intervals
    # Challenges
        # For certain intervals, timestamps from the start were missing. 
        # I found out that the timestamp data that I was sending to be aggregated was not in order by time, so some rows were being cutoff
        # To fix this, I sorted the data before I did any aggregation to it