import requests
import pandas as pd
from datetime import datetime, timedelta
from bs4 import BeautifulSoup
import numpy as np
import database
from helper_functions import convert_to_date, log_error

def determine_dates(year, season_start_date, season_end_date, table_name):
        '''
        Determines a start/end date to pull data for
        This is used for incremental loading. 
        If there is no data in the SQLite database for a certain season, we will use the season start/end dates
        If there is already data, we will pull the MAX date from the table. The start date will be MAX(date) - 1, and the end date will be the MIN(Current Date, Season End Date)
        '''
        current_max_date = database.get_max_date_for_season(table_name=table_name, season=year)
        if current_max_date:
            # Subtract 1 day from the max date to ensure idempotency
            current_max_date = convert_to_date(current_max_date)
            start_date = (current_max_date - timedelta(days=1)).strftime("%Y-%m-%d")
            end_date = min(convert_to_date(datetime.today()), season_end_date)
        else:
            start_date = season_start_date
            end_date = season_end_date
        print(f"""Scraper Operations - Calculating dates for the {year} season:
- Season: {year}
- Season Start Date: {season_start_date}
- Season End Date: {season_end_date}
- Current Max Date in Database: {current_max_date}
- API Start Date: {start_date} (start date for our data pull)
- API End Date: {end_date} (end date for our data pull)
              """)
        return start_date, end_date
        
def get_data(url, year, season_start_date, season_end_date, table_name, insert_data=True):
    '''
    Scrapes data from website for start/end dates and a specific season
    Loops through the HTML on each page to pull data and stops when there is no longer data displayed on the page
    '''

    season_start_date = convert_to_date(season_start_date)
    season_end_date = convert_to_date(season_end_date)
    start_date, end_date = determine_dates(year, season_start_date=season_start_date, season_end_date=season_end_date, table_name=table_name)
    start_num = 0
    page = 1
    if insert_data == True:
        table_data = []
        while True:
            base_url = url.format(start_date=start_date, end_date=end_date, start_num=start_num)
            try:
            # Fetch the webpage content
                response = requests.get(base_url)
                response.raise_for_status()  # Raises an HTTPError for bad responses (4xx or 5xx)

            except requests.exceptions.RequestException as e:
                print(f"Error fetching data: {e}")
                log_error(e)
                break  # Exit the loop if there’s an error

            # Parse the HTML content
            soup = BeautifulSoup(response.content, "html.parser")
            table = soup.find("table", class_="datatable center")

            # Check if the table exists and contains more than just headers
            if not table:
                print(f"Scraper Operations - No data found for {start_date} - {end_date} for the {year} season")
                break

            table_rows = table.find_all("tr")
            if len(table_rows) <= 1:  # Only headers are present
                print(f"Scraper Operations - Scraping stopped for {start_date} - {end_date} for the {year} season on page {page - 1}, as no more data was found on page {page}")
                break
            
            # Extract data from rows, excluding rows with the class 'DraftTableLabel'
            for row in table_rows:
                if 'DraftTableLabel' in row.get("class", []):
                    continue

                columns = row.find_all("td")
                row_data = [col.text.strip() for col in columns]
                row_data.insert(0, convert_to_date(datetime.today()))
                row_data.insert(1, year)
                if row_data:
                    table_data.append(row_data)

            # Increment start_num to load the next page
            start_num += 25
            page += 1
    return table_data, start_date, end_date

def clean_insert_raw_injuries(table_data, start_date, end_date, year, table_name):
    df = pd.DataFrame(data=table_data, columns=["date_added", "season", "report_date", "team", "acquired", "relinquished", "notes"])
    df = df.replace(r'^\s*$', np.nan, regex=True)
    df = df.replace(r'^•\s*', "", regex=True)
    database.incremental_load_delete(table_name=table_name, season=year, delete_date=start_date)
    database.insert_data_to_db(df=df, table_name=table_name)
    print(f'''
---------------------------------------------
SUCCESSLY FINISHED PROCESS FOR {year} INTO '{table_name}'
---------------------------------------------''')

