# Project: SofaScore Data Web Scraping

## Technologies and Version
- Python: 3.13.1
- Selenium: 4.32.0
- Selenium-Wire: 5.1.0
- Pandas: 2.2.3
- Psycopg2: 2.9.10
- Boto3: 1.38.23
- Python Dotenv: 1.1.0
- PostgreSQL 17

## Project Structure
- `setup_database.py`: Script for set up your PostgreSQL database with all the tables.
- `stage1_collect_match_keys.py`: The first script in the data pipeline.
- `stage2_extract_match_data.py`: The second script that uses the data from the first stage.
- `leagues_season.json`: A configuration file containing the leagues and seasons to be scraped.
- `keys_matches.csv`: Stores the match IDs and other keys scraped in Stage 1.
- `registered_leagues.csv`, `registered_seasons.csv`, `registered_teams.csv`: CSV files used to track processed IDs and prevent redundant database calls.
- `.env`: An environment file to store sensitive credentials (database and AWS keys). This file is ignored by Git.

## How it Works
The scrap pipeline is divided into two main stages:

### Stage 1: Collect Match Keys (`stage1_collect_match_keys.py`)
This script initiates the data collection process by reading the `leagues_season.json` file. It then uses Selenium to navigate to the SofaScore website and find all the matches for the specified leagues and seasons. The script performs the following tasks:
1. **Database Seeding:** Inserts initial data for leagues, seasons, and teams into a PostgreSQL database. It checks against local CSV files (`registered_leagues.csv`, etc.) to avoid unnecessary database calls.
2. **Match Key Extraction:** Extracts crucial match information, including 'customId', 'id', 'slug', and participating teams, for all ended matches.
3. **Local Storage:** Stores this extracted information into keys_matches.csv, which serves as the input for the next stage of the pipeline.

### Stage 2: Extract Match Data (`stage2_extract_match_data.py`)
This script uses the `keys_matches.csv` file created in the first stage. It iterates through each match key and navigates to the respective match page to perform a more detailed data extraction.
1. **Web Scraping:** It uses Selenium to intercept API requests made by the SofaScore website and extracts comprehensive match statistics and general match information.
2. **Data Processing:** The compressed API data (Gzip format) is decompressed and converted into a readable JSON format.
3. **Cloud Storage:** The processed JSON data for both match statistics (matche_stats/) and general information (matche_info/) is uploaded directly to an AWS S3 bucket for secure and scalable storage.

## Setup and Installation
1. **Clone the repository:**
   ```
   git clone https://github.com/AndreiRezende/sofascore-scrap
   cd  sofascore-scrap
   ```

2. **Install dependencies:**
   ```
   pip install -r requirements.txt
   ```

3. **Configure environment variables:**
   Create a `.env` file in the project's root directory with your credentials.
   ```
   AWS_ACCESS_KEY_ID="your_access_key_id"
   AWS_SECRET_ACCESS_KEY="your_secret_access_key"
   USER_POSTGRES="your_postgres_user"
   PASSWORD_POSTGRES="your_postgres_password"
   HOST_POSTGRES="your_postgres_host"
   PORT_POSTGRES="your_postgres_port"
   DATABASE_POSTGRES="your_postgres_database"
   ```

4. **Run the pipeline:**
   First, run the set up database script:
   ```
   python setup_database.py
   ```
   Next, run the script for the first stage:
   ```
   python stage1_collect_match_keys.py
   ```
   Finally, run the second stage script:
   ```
   python stage2_extract_match_data.py
   ```