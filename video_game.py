import pandas as pd
import sqlite3
import uuid
import re
import sys

def load_and_clean_data(vgsales_file, games_file):
    """
    Loads two CSV files, cleans the data, and returns two DataFrames.
    """
    print("Loading and cleaning data...")
    try:
        df_vgsales = pd.read_csv(vgsales_file)
        df_games = pd.read_csv(games_file)
    except FileNotFoundError as e:
        print(f"Error: A required file was not found. Please ensure '{vgsales_file}' and '{games_file}' are in the same directory.")
        sys.exit(1)

    # --- Preprocessing and Cleaning for vgsales.csv ---
    df_vgsales.dropna(subset=['Year', 'Publisher', 'Name', 'Platform', 'Genre'], inplace=True)
    df_vgsales.drop_duplicates(inplace=True)
    df_vgsales['Year'] = pd.to_numeric(df_vgsales['Year'], errors='coerce').astype('Int64')
    df_vgsales.dropna(subset=['Year'], inplace=True)
    sales_cols = ['NA_Sales', 'EU_Sales', 'JP_Sales', 'Other_Sales', 'Global_Sales']
    for col in sales_cols:
        df_vgsales[col] = pd.to_numeric(df_vgsales[col], errors='coerce')
    df_vgsales.dropna(subset=sales_cols, inplace=True)
    df_vgsales['Platform'] = df_vgsales['Platform'].str.lower().str.strip()
    df_vgsales['Genre'] = df_vgsales['Genre'].str.lower().str.strip()
    df_vgsales['Publisher'] = df_vgsales['Publisher'].str.lower().str.strip()

    # --- Preprocessing and Cleaning for games.csv ---
    df_games['Rating'] = df_games['Rating'].apply(
        lambda x: float(re.sub(r'[^0-9.]', '', str(x))) if re.sub(r'[^0-9.]', '', str(x)) else None
    )
    df_games.dropna(subset=['Rating', 'Release Date', 'Number of Reviews'], inplace=True)
    df_games.drop_duplicates(inplace=True)
    df_games['Release Date'] = pd.to_datetime(df_games['Release Date'], format="%b %d, %Y", errors='coerce')
    df_games['Number of Reviews'] = pd.to_numeric(
        df_games['Number of Reviews'].apply(lambda x: re.sub(r'[^0-9]', '', str(x))),
        errors='coerce'
    ).astype('Int64')
    df_games.dropna(subset=['Release Date', 'Number of Reviews'], inplace=True)
    df_games['Title'] = df_games['Title'].str.lower().str.strip()
    
    # Create a unique game_id for joining
    df_vgsales['game_id'] = [str(uuid.uuid4()) for _ in range(len(df_vgsales))]
    df_games['game_id'] = [str(uuid.uuid4()) for _ in range(len(df_games))]
    
    print("Data cleaning complete!")
    return df_vgsales, df_games

def setup_sqlite_database(df_vgsales, df_games):
    """
    Connects to SQLite, creates tables, and populates them from DataFrames.
    """
    print("Connecting to SQLite database...")
    try:
        # Connect to a new or existing SQLite database file
        conn = sqlite3.connect('video_games.db')
        cursor = conn.cursor()

        print("Creating tables...")
        # Create the 'games' table
        cursor.execute("DROP TABLE IF EXISTS games")
        cursor.execute("""
            CREATE TABLE games (
                game_id TEXT PRIMARY KEY,
                title TEXT,
                release_date TEXT
            )
        """)
        
        # Create the 'sales_data' table
        cursor.execute("DROP TABLE IF EXISTS sales_data")
        cursor.execute("""
            CREATE TABLE sales_data (
                rank INTEGER PRIMARY KEY,
                game_id TEXT,
                name TEXT,
                platform TEXT,
                year INTEGER,
                genre TEXT,
                publisher TEXT,
                na_sales REAL,
                eu_sales REAL,
                jp_sales REAL,
                other_sales REAL,
                global_sales REAL,
                FOREIGN KEY (game_id) REFERENCES games(game_id)
            )
        """)

        # Create the 'ratings_data' table
        cursor.execute("DROP TABLE IF EXISTS ratings_data")
        cursor.execute("""
            CREATE TABLE ratings_data (
                game_id TEXT PRIMARY KEY,
                rating REAL,
                number_of_reviews INTEGER,
                genres TEXT,
                summary TEXT,
                FOREIGN KEY (game_id) REFERENCES games(game_id)
            )
        """)
        print("Tables created successfully.")
        
        print("Populating tables with data...")
        # Populate the 'games' table first
        df_games_combined = pd.concat([
            df_vgsales[['game_id', 'Name']].rename(columns={'Name': 'title'}),
            df_games[['game_id', 'Title']].rename(columns={'Title': 'title'})
        ])
        df_games_combined['release_date'] = df_games_combined.apply(
            lambda row: df_games.loc[df_games['game_id'] == row['game_id'], 'Release Date'].iloc[0] 
                        if row['game_id'] in df_games['game_id'].values else None,
            axis=1
        )
        df_games_combined.drop_duplicates(subset=['game_id'], inplace=True)
        df_games_combined.to_sql('games', conn, if_exists='append', index=False)
        
        # Populate the 'sales_data' table
        df_vgsales.to_sql('sales_data', conn, if_exists='append', index=False)

        # Populate the 'ratings_data' table
        df_games.to_sql('ratings_data', conn, if_exists='append', index=False)

        conn.commit()
        print("All data inserted successfully!")

    except sqlite3.Error as err:
        print(f"Error during database setup: {err}")
        conn.rollback()
    finally:
        if 'conn' in locals():
            conn.close()
            print("SQLite connection closed.")

if __name__ == '__main__':
    df_vgsales, df_games = load_and_clean_data("vgsales.csv", "games.csv")
    setup_sqlite_database(df_vgsales, df_games)
