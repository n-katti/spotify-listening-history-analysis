import helper_functions
import config
import database
import scraper

# Load in config variables
season_dates = config.SEASON_DATES
raw_ir_table = config.RAW_IR_TABLE_NAME
raw_injuries_table = config.RAW_INJURIES_TABLE_NAME
dim_season_calendar_table = config.SEASON_CALENDAR_TABLE_NAME


# Populate dim_calendar_dates. This uses the start/end dates of each season to create a dim table at the daily grain
# Contains the season, week number, date, week start date and week end dates
def create_season_calendar():
    calendar_dates_df = helper_functions.generate_calendar(season_dates)
    database.insert_data_to_db(df=calendar_dates_df, table_name=dim_season_calendar_table)

def get_ir_data():
    database.create_backup_table(table_name=raw_ir_table)
    for year, dates in season_dates.items():
        url = "https://prosportstransactions.com/football/Search/SearchResults.php?Player=&Team=&BeginDate={start_date}&EndDate={end_date}&ILChkBx=yes&submit=Search&start={start_num}"
        season_start_date = dates['start_date']
        season_end_date = dates['end_date']
        table_data, start_date, end_date = scraper.get_data(url=url, year=year, season_start_date=season_start_date, season_end_date=season_end_date, table_name=raw_ir_table, insert_data=True)
        scraper.clean_insert_raw_injuries(table_data=table_data, start_date=start_date, end_date=end_date, year=year, table_name=raw_ir_table)

def get_injuries_data():
    database.create_backup_table(table_name=raw_injuries_table)
    for year, dates in season_dates.items():
        url = "https://prosportstransactions.com/football/Search/SearchResults.php?Player=&Team=&BeginDate={start_date}&EndDate={end_date}&InjuriesChkBx=yes&submit=Search&start={start_num}"
        season_start_date = dates['start_date']
        season_end_date = dates['end_date']
        table_data, start_date, end_date = scraper.get_data(url=url, year=year, season_start_date=season_start_date, season_end_date=season_end_date, table_name=raw_injuries_table, insert_data=True)
        scraper.clean_insert_raw_injuries(table_data=table_data, start_date=start_date, end_date=end_date, year=year, table_name=raw_injuries_table)
# print(database.get_max_date_for_season(raw_ir_table, 2023))

if __name__ == '__main__':
    # Run Create Table queries
    database.create_tables()

    # Populate dim_calendar_dates. This uses the start/end dates of each season to create a dim table at the daily grain
    # Contains the season, week number, date, week start date and week end dates
    # create_season_calendar()
    # print(database.table_to_df(raw_ir_table))
    # Scrapes raw IR data and inserts into SQLite database. Creates a duplicate backup table beforehand in case we need to revert any changes
    get_ir_data()
    get_injuries_data()
    # Used for testing to see the contents of the SQLite tables
    # print(database.table_to_df("season_calendar"))
    # print(database.table_to_df(raw_ir_table))
    # print(database.table_to_df(raw_injuries_table))
    # print(database.table_to_df(f"raw_injuries_backup"))