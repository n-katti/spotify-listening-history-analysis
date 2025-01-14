import pandas as pd
from datetime import datetime, timedelta
import logging
import os
import traceback

# Determine the log file directory and ensure it exists
script_dir = os.path.dirname(os.path.abspath(__file__))  
log_dir = os.path.join(script_dir, "../logging")         
os.makedirs(log_dir, exist_ok=True)                     

log_file_path = os.path.join(log_dir, "errors.log")

# Configure logging
logging.basicConfig(
    filename=log_file_path,
    level=logging.ERROR,
    format="%(asctime)s - %(levelname)s - %(message)s"
)


def log_error(exception, context=None):
    """Logs an exception, its traceback, and optional context."""
    logging.error(f"Error: {exception}")
    if context:
        logging.error(f"Context: {context}")
    logging.error("Traceback:", exc_info=True)


def convert_to_date(date_string):
    if isinstance(date_string, datetime):
        return date_string.date()
    else:
        return datetime.strptime(date_string, "%Y-%m-%d").date()

def generate_calendar(date_dict: dict):
    '''
    Generates a dataframe based on each season's start and end dates with corresponding Week numbers that align to the NFL season
    Weeks begin Tuesday and end the following Monday
    '''
    weeks_data = []

    for year, dates in date_dict.items():
        start_date = convert_to_date(dates['start_date'])
        end_date = convert_to_date(dates['end_date'])

        # Adjust the start date to the previous Tuesday to start on a Tuesday since NFL weeks go Tuesday - Monday
        adjusted_start_date = start_date - timedelta(days=(start_date.weekday() - 1) % 7)

        # Initialize a list to store each week's data
        current_week_start_date = adjusted_start_date
        current_date = adjusted_start_date
        week_num = 1

        # Loop to generate each week's start and end dates until the end date is reached
        while current_week_start_date <= end_date:
            current_week_end_date = current_week_start_date + timedelta(days=6)
            while current_date <= current_week_end_date:
            # Append week data to the list
                weeks_data.append({
                    "season": year,
                    "week_number": week_num,
                    "report_date" : current_date,
                    "week_start_date": current_week_start_date, 
                    "week_end_date": current_week_end_date, 
                })

                current_date += timedelta(days=1)
            
            # Move to the next week (Tuesday of the following week)
            current_week_start_date += timedelta(days=7)
            week_num += 1

    # Create DataFrame from the list of weeks
    df = pd.DataFrame(weeks_data)

    # Display the DataFrame
    return df

