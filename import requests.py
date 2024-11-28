# # https://prosportstransactions.com/football/Search/SearchResults.php?Player=&Team=&BeginDate=2024-01-01&EndDate=2024-02-01&ILChkBx=yes&submit=Search

import requests
import pandas as pd
from datetime import datetime, timedelta
from bs4 import BeautifulSoup
import numpy as np

def generate_calendar(variables: dict):
    '''
    Generates a dataframe based on each season's start and end dates with corresponding Week numbers that align to the NFL season
    Weeks begin Tuesday and end the following Monday
    '''
    weeks_data = []

    for year, data in variables.items():
        start_date = datetime.strptime(data['start_date'], "%Y-%m-%d")
        end_date = datetime.strptime(data['end_date'], "%Y-%m-%d")

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
                    "Year": year,
                    "Week Number": week_num,
                    "Date" : current_date,
                    "Week Start Date": current_week_start_date,
                    "Week End Date": current_week_end_date
                })

                current_date += timedelta(days=1)
            
            # Move to the next week (Tuesday of the following week)
            current_week_start_date += timedelta(days=7)
            week_num += 1

    # Create DataFrame from the list of weeks
    df = pd.DataFrame(weeks_data)

    # Display the DataFrame
    return df

variables = {
    2023: {"start_date": "2023-09-07",
        #   "end_date": "2024-02-11"}, 
         "end_date": "2023-09-20"}, 
    # 2024: {"start_date": "2024-09-05",
    #         "end_date": "2025-02-09"}, 
    2024: {"start_date": "2024-09-20",
            "end_date": "2024-09-30"}, 
}



def get_data(variables: dict):
    table_data = []
    for year, data in variables.items():

        start_date = data['start_date']
        end_date = data['end_date']
        start_num = 0
        page = 1

        while True:
            base_url = f"https://prosportstransactions.com/football/Search/SearchResults.php?Player=&Team=&BeginDate={start_date}&EndDate={end_date}&ILChkBx=yes&submit=Search&start={start_num}"
            try:
            # Fetch the webpage content
                response = requests.get(base_url)
                response.raise_for_status()  # Raises an HTTPError for bad responses (4xx or 5xx)

            except requests.exceptions.RequestException as e:
                print(f"Error fetching data: {e}")
                break  # Exit the loop if there’s an error

            # Parse the HTML content
            soup = BeautifulSoup(response.content, "html.parser")
            table = soup.find("table", class_="datatable center")

            # Check if the table exists and contains more than just headers
            if not table:
                print(f"No data table found on page {page}")
                break

            table_rows = table.find_all("tr")
            if len(table_rows) <= 1:  # Only headers are present
                print(f"No data table found on page {page}")
                break

            # Extract data from rows, excluding rows with the class 'DraftTableLabel'
            for row in table_rows:
                if 'DraftTableLabel' in row.get("class", []):
                    continue

                columns = row.find_all("td")
                row_data = [col.text.strip() for col in columns]
                if row_data:
                    table_data.append(row_data)

            # Increment start_num to load the next page
            start_num += 25
            page += 1

    df = pd.DataFrame(data=table_data, columns=["Date", "Team", "Acquired", "Relinquished", "Notes"])
    df = df.replace(r'^\s*$', np.nan, regex=True)
    df = df.replace(r'^•\s*', "", regex=True)
    return df

df = get_data(variables=variables)
df = df[~df['Notes'].str.contains('activated', case=False, na=False)]
print(df["Notes"].drop_duplicates())

# # df = generate_calendar(variables=variables)