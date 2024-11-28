# database.py
import sqlite3
import config

class Database:
    def __init__(self):
        # Initialize the database connection and cursor
        self.connection = sqlite3.connect(config.DATABASE_PATH)
        self.cursor = self.connection.cursor()

    def get_connection(self):
        return self.connection

    def get_cursor(self):
        return self.cursor

    def close(self):
        # Ensure that we close the connection when done
        if self.connection:
            self.connection.close()

# Initialize the database connection once
db = Database()

# Function to get the max date from the database for a specific season
def get_max_date_from_db(season_year):
    cursor = db.get_cursor()
    cursor.execute("SELECT MAX(date) FROM data WHERE season_year = ?", (season_year,))
    max_date = cursor.fetchone()[0]
    return max_date

# Function to load data into the database
def load_data_into_db(data, season_year):
    cursor = db.get_cursor()
    for record in data:
        cursor.execute("""
            INSERT INTO data (date, season_year, other_columns)
            VALUES (?, ?, ?)
        """, (record['date'], season_year, record['other_columns']))
    db.get_connection().commit()

# Function to clean data for a specific season
def clean_data_for_season(season_year, start_date):
    cursor = db.get_cursor()
    adjusted_start_date = (datetime.strptime(start_date, "%Y-%m-%d") - timedelta(days=1)).strftime("%Y-%m-%d")
    cursor.execute("DELETE FROM data WHERE season_year = ? AND date > ?", (season_year, adjusted_start_date))
    db.get_connection().commit()
