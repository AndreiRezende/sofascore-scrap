import psycopg2
from psycopg2 import sql
import os
from dotenv import load_dotenv

# Load environment variables from a .env file
load_dotenv() 

# Get database credentials from environment variables
DB_CREDENTIALS = {
    'dbname': os.environ.get('DATABASE_POSTGRES'),
    'user': os.environ.get('USER_POSTGRES'),
    'password': os.environ.get('PASSWORD_POSTGRES'),
    'host': os.environ.get('HOST_POSTGRES'),
    'port': os.environ.get('PORT_POSTGRES')
}

def create_table_if_not_exists(db_params, table_name, columns_definition):
    """
    Creates a table in the PostgreSQL database if it does not already exist.

    Args:
        db_params (dict): A dictionary with database connection params.
        table_name (str): The name of the table to be created.
        columns_definition (str): The SQL definition of the table's columns.
    """
    conn = None
    try:
        # 1. Connect to the database
        conn = psycopg2.connect(**db_params)
        cursor = conn.cursor()

        # 2. SQL command to create the table with the IF NOT EXISTS clause
        create_table_sql = sql.SQL("CREATE TABLE IF NOT EXISTS {table_name} ({columns_definition});").format(
            table_name=sql.Identifier(table_name),
            columns_definition=sql.SQL(columns_definition)
        )

        # 3. Execute the SQL command
        cursor.execute(create_table_sql)
        print(f"Table '{table_name}' checked/created successfully.")

        # 4. Commit the changes
        conn.commit()

    except psycopg2.Error as e:
        print(f"Error connecting to the database or executing the query: {e}")

    finally:
        # 5. Close the connection
        if conn:
            conn.close()

# --- Main Script Execution ---

print("Starting table verification and creation...")

# Table 'player'
create_table_if_not_exists(DB_CREDENTIALS, 'player', 'id INTEGER PRIMARY KEY, name VARCHAR(255) NOT NULL, country VARCHAR(255)')

# Table 'team'
create_table_if_not_exists(DB_CREDENTIALS, 'team', 'id INTEGER PRIMARY KEY, name VARCHAR(255) NOT NULL, country VARCHAR(255), abbreviation VARCHAR(10)')

# Table 'league'
create_table_if_not_exists(DB_CREDENTIALS, 'league', 'id INTEGER PRIMARY KEY, name VARCHAR(255) NOT NULL, country VARCHAR(255)')

# Table 'season' with a foreign key
create_table_if_not_exists(DB_CREDENTIALS, 'season', 'id INTEGER PRIMARY KEY, season_year VARCHAR(255) NOT NULL, id_league INTEGER REFERENCES league(id)')

# Table 'referee'
create_table_if_not_exists(DB_CREDENTIALS, 'referee', 'id INTEGER PRIMARY KEY, name VARCHAR(255) NOT NULL, games_officiated INTEGER, yellow_cards INTEGER, red_cards INTEGER')

# Table 'stadium'
create_table_if_not_exists(DB_CREDENTIALS, 'stadium', 'id INTEGER PRIMARY KEY, name VARCHAR(255) NOT NULL, city VARCHAR(255), country VARCHAR(255), capacity INTEGER')

# Table 'match'
create_table_if_not_exists(DB_CREDENTIALS, 'match', """
    id INTEGER PRIMARY KEY,
    match_time TIMESTAMP NOT NULL,
    round VARCHAR(50),
    season_id INTEGER REFERENCES season(id),
    referee_id INTEGER REFERENCES referee(id),
    stadium_id INTEGER REFERENCES stadium(id),
    home_team_id INTEGER REFERENCES team(id),
    away_team_id INTEGER REFERENCES team(id)
""")

# Table 'match_stat'
create_table_if_not_exists(DB_CREDENTIALS, 'match_stat', 'id SERIAL PRIMARY KEY, stat_name VARCHAR(255) NOT NULL, stat_value INTEGER NOT NULL, matche_id INTEGER REFERENCES match(id)')

print("All tables have been checked and created.")