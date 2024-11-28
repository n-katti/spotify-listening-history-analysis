import helper_functions
import config
import database
import scraper


season_dates = config.SEASON_DATES
injury_table = config.INJURY_TABLE_NAME
calendar_table = config.CALENDAR_TABLE_NAME

# database.create_tables()
# calendar_dates_df = helper_functions.generate_calendar(season_dates)
# database.insert_data_to_db(df=calendar_dates_df, table_name=calendar_table)

# print(database.table_to_df("season_calendar"))

# scraper.get_data(date_dict=season_dates, table_name=injury_table, insert_data=True)
# print(database.table_to_df(injury_table))
# print(database.table_to_df(f"raw_injuries_backup"))
scraper.get_data(date_dict=season_dates, table_name=injury_table, insert_data=False)