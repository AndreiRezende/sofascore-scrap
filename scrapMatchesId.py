import psycopg2
from psycopg2 import Error
from seleniumwire import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import time
import gzip
import io
import json
import re
import csv
import os
from dotenv import load_dotenv

load_dotenv() # Load .env variables

# Read environment variables
user_postgres = os.environ.get('USER_POSTGRES')
password_postgres = os.environ.get('PASSWORD_POSTGRES')
host_postgres = os.environ.get('HOST_POSTGRES')
port_postgres = os.environ.get('PORT_POSTGRES')
database_postgres = os.environ.get('DATABASE_POSTGRES')

# Configure Chrome optioons
options = Options()  
options.headless = False
options.add_argument("--disable-blink-features=AutomationControlled")

# Initialize the WebDriver
driver = webdriver.Chrome(options=options)

# Set wait timeouts
driver.set_page_load_timeout(180)
wait = WebDriverWait(driver, 30)

# CSV file names to store processed IDs
# The idea behind this is to avoid reprocessing and saving data
# that has already been saved to the database, which helps to reduce database calls and save resources.
# But I'm nmot sure if it's more cost-effective to handle duplicate checks by manipulating local CSV files
# than by letting the database handle insertion errors for existing records.
csv_keys_matches = 'keys_matches.csv'
registered_leagues = 'registered_leagues.csv'
registered_seasons = 'registered_seasons.csv'
registered_teams = 'registered_teams.csv'

# Sets to store IDs in memory for fast existence checks
ids_keys_matches = set()
registered_leagues_ids = set()
registered_seasons_ids = set()
registered_teams_ids = set()


# Loads IDs from a CSV file into a set for future checks
def load_ids_from_csv(filename, id_set):
    if os.path.exists(filename):
        with open(filename, mode='r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row_dict in reader:
                if 'id' in row_dict:
                    id_set.add(row_dict['id'])
    print(f"Loaded {len(id_set)} IDs from {filename}")

# Appends a single ID to a CSV file
def write_id_to_csv(filename, id_value):
    with open(filename, mode='a', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=['id'])

        if os.stat(filename).st_size == 0:
            writer.writeheader()

        writer.writerow({'id': id_value})
    print(f"ID '{id_value}' added to '{filename}' CSV")

# Load all IDs of items that have already been processed and saved
load_ids_from_csv(csv_keys_matches, ids_keys_matches)
load_ids_from_csv(registered_leagues, registered_leagues_ids)         
load_ids_from_csv(registered_seasons, registered_seasons_ids)
load_ids_from_csv(registered_teams, registered_teams_ids)

# Read the JSON file with the league's championship data
with open('leagues_season.json', 'r', encoding='utf-8') as f:
    leagues = json.load(f)

conn = None
cursor = None

try:
    # Connect to the database
    conn = psycopg2.connect(
        user=user_postgres,
        password=password_postgres,
        host=host_postgres,
        port=port_postgres,
        database=database_postgres
    )
    cursor = conn.cursor()

    print("Succesfully connected to the database")

    # SQL INSERT queries
    insert_league = "INSERT INTO league (id, name, country) VALUES (%s, %s, %s)"
    insert_season = "INSERT INTO season (id, id_league, season_year) VALUES (%s, %s, %s)"
    insert_team = "INSERT INTO team (id, name, team_abbreviation, country) VALUES (%s, %s, %s, %s)"

except (Exception, Error) as error:
    print("Error to coneccting to the database: ", error)
    driver.quit()
    exit()

try:
    # Process all leagues registered in CSV file
    for league in leagues:
        # Configuration of the league and season selected for extraction
        country = league.get('country')
        slug = league.get('slug')
        name = league.get('name')
        season = league.get('season')
        id_season = league.get('id_season')
        id_league = league.get('id_league')
        leagueSeason = f"{slug}-{season}"

        if str(id_league) not in registered_leagues_ids: # Check if Already Been Saved (CABS)
            try:
                values = (id_league, name, country)
                cursor.execute(insert_league, values)

                if cursor.rowcount > 0:
                    conn.commit()
                    registered_leagues_ids.add(id_league)
                    write_id_to_csv(registered_leagues, id_league)
                    print(f"League {name} ({id_league}) inserted")
                else:
                    print(f"League {name} ({id_league}) already exists in DB")
                    registered_leagues_ids.add(id_league)

            except Exception as e:
                conn.rollback()
                print(f"Error inserting league {name} ({id_league}): {e}")
        else:
            print(f"League {name}({id_league}) already processed")

        if str(id_season) not in registered_seasons_ids: # CABS
            try:
                values = (id_season, id_league, season)
                cursor.execute(insert_season, values)

                if cursor.rowcount > 0:
                    conn.commit()
                    registered_seasons_ids.add(id_season)
                    write_id_to_csv(registered_seasons, id_season)
                    print(f"Season {season} ({id_season}) for league '{name}' inserted")
                else:
                    print(f"Season {season} ({id_season}) for league '{name}' already exists in DB")

            except Exception as e:
                conn.rollback()
                print(f"Error inserting season {season} ({id_season}) for league '{name}': {e}")
        else:
            print(f"Season {season} ({id_season}) for league '{name}' already processed")


        try:
            print(f"Acessing SofaScore: {leagueSeason}")
            driver.maximize_window()
            driver.get(f"https://www.sofascore.com/pt/torneio/futebol/{country}/{slug}/{id_league}#id:{id_season}")
            time.sleep(20)

            # Find the element that shows the current round and extract its number
            element = driver.find_element(By.XPATH, '/html/body/div[1]/main/div[2]/div/div/div[1]/div[4]/div[1]/div[1]/div[3]/div/div/div[1]/div/div/button/div/div')
            texto = element.text
            match = re.search(r'\d+', texto)
            current_round = int(match.group())

            # Find and click the 'Back' and 'Next' buttons to load the API data
            backButton = wait.until(EC.element_to_be_clickable((By.XPATH, "/html/body/div[1]/main/div[2]/div/div/div[1]/div[4]/div[1]/div[1]/div[3]/div/div/div[1]/div/button[1]")))
            backButton.click()
            driver.requests.clear()
            time.sleep(5)
            nextButton= wait.until(EC.element_to_be_clickable((By.XPATH, "/html/body/div[1]/main/div[2]/div/div/div[1]/div[4]/div[1]/div[1]/div[3]/div/div/div[1]/div/button[2]")))
            nextButton.click()
            time.sleep(5)

            # Process all rounds from current down to round 1 (inclusive)
            while current_round >= 1:
                print(current_round)
                api_response = None

                for request in driver.requests:
                    if request.response:
                        if (f'round/{current_round}') in request.url: # Check for the API URL containing round information
                            print(f"Detected: {request.url}")
                            print(f"Status: {request.response.status_code}")

                            compressed_body = request.response.body

                            try:
                                # Decompress Gzip
                                with gzip.GzipFile(fileobj=io.BytesIO(compressed_body)) as f:
                                    decompressed_data = f.read()

                                json_data = json.loads(decompressed_data.decode('utf-8'))

                                # Extraction the target data
                                for event in json_data.get('events', []):
                                    if str(event['id']) not in ids_keys_matches:   # CABS

                                        if all(k in event for k in ['customId', 'id', 'slug', 'homeTeam', 'awayTeam']) and event['status']['description'] == 'Ended':
                                            # Extracting home and away team IDs and info
                                            home_id = event['homeTeam']['id']
                                            home_name = event['homeTeam']['name']
                                            home_code = event['homeTeam']['nameCode']
                                            home_country = event['homeTeam']['country']['name']
                                            away_id = event['awayTeam']['id']
                                            away_name = event['awayTeam']['name']
                                            away_code = event['awayTeam']['nameCode']
                                            away_country = event['awayTeam']['country']['name']

                                            if str(event['homeTeam']['id']) not in registered_teams_ids: # CABS
                                                # Save home team in the database
                                                try:
                                                    team = (
                                                        home_id,
                                                        home_name,
                                                        home_code,
                                                        home_country
                                                    )
                                                    cursor.execute(insert_team, team)

                                                    if cursor.rowcount > 0:
                                                        conn.commit()
                                                        registered_teams_ids.add(home_id)
                                                        write_id_to_csv(registered_teams, home_id)
                                                        print(f"Team {home_name} ({home_id}) inserted")
                                                    else:
                                                        print(f"Team {home_name} ({home_id}) already exists in DB")      
                                                except Exception as e:
                                                    conn.rollback()
                                                    print(f"Error inserting team {home_name} ({home_id}): {e}")
                                            else:
                                                print(f"Team {home_name} ({home_id}) already processed")
                                            
                                            # A single round is enough to scrape and save the competition's teams
                                            if current_round == 1: 
                                                if str(event['awayTeam']['id']) not in registered_teams_ids: # CABS
                                                    # Save away team in the database
                                                    try:
                                                        team = (
                                                            away_id,
                                                            away_name,
                                                            away_code,
                                                            away_country
                                                        )
                                                        cursor.execute(insert_team, team)

                                                        if cursor.rowcount > 0:
                                                            conn.commit()
                                                            registered_teams_ids.add(away_id)
                                                            write_id_to_csv(registered_teams, away_id)
                                                            print(f"Team {away_name} ({away_id}) inserted")
                                                        else:
                                                            print(f"Team {away_name} ({away_id}) already exists in DB")      
                                                    except Exception as e:
                                                        conn.rollback()
                                                        print(f"Error inserting team {away_name} ({away_id}): {e}")
                                                else:
                                                    print(f"Team {away_name} ({away_id}) already processed")
                                            
                                            result = {
                                                'customId': event['customId'],
                                                'id': str(event['id']),
                                                'id_mandante': event['homeTeam']['id'],
                                                'id_visitante': event['awayTeam']['id'],
                                                'slug': event['slug'],
                                                'league': leagueSeason,
                                                'mandante': event['homeTeam']['name'],
                                                'visitante': event['awayTeam']['name'],
                                                'rodada': current_round
                                            }

                                            # Create csv file and write the results keys
                                            with open(csv_keys_matches, mode='a', newline='', encoding='utf-8') as f:
                                                writer = csv.DictWriter(f, fieldnames=['customId', 'id', 'id_mandante', 'id_visitante', 'slug', 'league', 'mandante', 'visitante', 'rodada'])

                                                if os.stat(csv_keys_matches).st_size == 0:
                                                    writer.writeheader()

                                                writer.writerow(result)
                                                ids_keys_matches.add(result['id'])  # Update set ids

                                            print(f"Salvo: {result}")
                                    else:
                                        print(f"Match {event['id']} already saved")

                            except Exception as e:
                                print(f"Error decompressing or processing JSON: {e}")

                driver.requests.clear()

                if current_round > 1:
                    backButton.click()

                # Navigate to previous round
                current_round = current_round - 1
                time.sleep(10)

        except Exception as e:
            print(f"Error acessing {leagueSeason}: {e}")
finally:
    if cursor is not None:
        cursor.close()
        print("Cursor closed.")
    if conn is not None and not conn.closed:
        conn.close()
        print("Database connection closed.")

driver.quit()