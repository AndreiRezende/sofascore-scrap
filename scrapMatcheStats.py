from seleniumwire import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from botocore.exceptions import ClientError
import time
import gzip
import io
import json
import boto3
import pandas as pd
import os
from dotenv import load_dotenv

load_dotenv() # Load .env variables

# Read environment variables
aws_key_id = os.environ.get('AWS_ACCESS_KEY_ID')
aws_secret_key = os.environ.get('AWS_SECRET_ACCESS_KEY')

def upload_to_s3(json_obj, bucket_name, s3_filename, aws_access_key_id, aws_secret_access_key, region='us-east-2'):
    s3 = boto3.client(
        's3',
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
        region_name=region
    )

    try:
        # Check if the object exists
        s3.head_object(Bucket=bucket_name, Key=s3_filename)
        print(f"File already exists: s3://{bucket_name}/{s3_filename}. Skipping upload.")
        return # Return early if the file exists
    except ClientError as e:
        # A 404 error means the file wasn't found
        if e.response['Error']['Code'] == '404':
            pass
        else:
            # For other types of errors, re-throw or handle them
            raise e

    # If it reached this point, the file doesn't exist and can be loaded
    json_str = json.dumps(json_obj, ensure_ascii=False, indent=2)
    
    s3.put_object(
        Bucket=bucket_name,
        Key=s3_filename,
        Body=json_str.encode('utf-8'),
        ContentType='application/json'
    )
    print(f"Data (JSON) pushed to S3 storage: s3://{bucket_name}/{s3_filename}")

# Configure Chrome options
options = Options()
options.headless = True
options.add_argument("--disable-blink-features=AutomationControlled")

# Initialize the WebDriver
driver = webdriver.Chrome(options=options)

# Set wait timeouts
driver.set_page_load_timeout(180)
wait = WebDriverWait(driver, 30)

# Read the CSV file containing match keys and create a DataFrame
df_matches = pd.read_csv('keys_matches.csv')

for match in df_matches.itertuples(index=False):
    # Configuration off the match selected for the extraction
    name_match = match.slug
    custom_id = match.customId
    id_match = match.id
    league = match.league

    url = f"https://www.sofascore.com/pt/football/match/{name_match}/{custom_id}#id:{id_match}"

    try:
        print(f"Acessing Match {id_match}")
        driver.maximize_window()
        driver.get(url)
        time.sleep(10)

        # Accept Cookies
        try:
            driver.find_element(By.XPATH, '//button[contains(text(),"Accept")]').click()
            time.sleep(1)
        except:
            pass
        
        # Try to click in the stats button to trigger the API request
        try:
            driver.requests.clear()
            driver.find_element(By.XPATH, "/html/body/div[1]/main/div[2]/div/div/div[1]/div[4]/div[2]/div[1]/div/div[1]/div/div/div/h2[2]").click()
            time.sleep(10)
        except Exception as e:
            print(f"Error Browse the stats: {e}")

        for request in driver.requests:
            if request.response:
                # Check for the specific stats API URL
                if(f"https://www.sofascore.com/api/v1/event/{id_match}/statistics") == request.url:
                    compressed_body = request.response.body
                    try:
                        # Decompress Gzip data and load JSON
                        with gzip.GzipFile(fileobj=io.BytesIO(compressed_body)) as f:
                            decompressed_data = f.read()
                        json_data = json.loads(decompressed_data.decode('utf-8'))

                        # Find the 'ALL' period statistics
                        period_all = next((item for item in json_data['statistics'] if item['period'] == 'ALL'), None)
                        
                        if period_all:
                             # Upload the stats data to S3
                             upload_to_s3(
                                json_obj=period_all,
                                bucket_name="sofascore-scrap-project",
                                s3_filename=f"matche_stats/{league}/{name_match}-{id_match}-period-all.json",
                                aws_access_key_id=aws_key_id,
                                aws_secret_access_key=aws_secret_key
                            )
                    except Exception as e:
                        print(f"Error decompressing or processing JSON: {e}")
                
                # Check for the specific match info API URL
                if(f"https://www.sofascore.com/api/v1/event/{id_match}") == request.url:
                    compressed_body = request.response.body
                    try:
                        # Decompress Gzip data and load JSON
                        with gzip.GzipFile(fileobj=io.BytesIO(compressed_body)) as f:
                            decompressed_data = f.read()

                        json_data = json.loads(decompressed_data.decode('utf-8'))

                        # Upload the general match info to S3
                        upload_to_s3(
                            json_obj=json_data,
                            bucket_name="sofascore-scrap-project",
                            s3_filename=f"matche_info/{league}/{name_match}-{id_match}-info.json",
                            aws_access_key_id=aws_key_id,
                            aws_secret_access_key=aws_secret_key
                        )
                    except Exception as e:
                        print(f"Error decompressing or processing JSON: {e}")
                

        # Try to click in the lineup button to trigger the API request
        '''
        try:
            driver.requests.clear()
            driver.find_element(By.XPATH, "/html/body/div[1]/main/div[2]/div/div/div[1]/div[4]/div[2]/div[1]/div/div[1]/div/div/div/h2[1]").click()
            time.sleep(10)
        except Exception as e:
            print(f"Error Browse the lineup: {e}")

        for request in driver.requests:
            if request.response:
                # Check for the specific lineups API URL
                if(f"https://www.sofascore.com/api/v1/event/{id_match}/lineups") == request.url:
                    compressed_body = request.response.body
                    try:
                        # Decompress Gzip data and load JSON
                        with gzip.GzipFile(fileobj=io.BytesIO(compressed_body)) as f:
                            decompressed_data = f.read()

                        json_data = json.loads(decompressed_data.decode('utf-8'))
                        
                        # Upload players stats data to S3
                        upload_to_s3(
                            json_obj=json_data,
                            bucket_name="sofascore-scrap-project",
                            s3_filename=f"matche_players_stats/{league}/{name_match}-{id_match}-players-stats.json",
                            aws_access_key_id=aws_key_id,
                            aws_secret_access_key=aws_secret_key
                        )

                    except Exception as e:
                        print(f"Error decompressing or processing JSON: {e}")         
            else:
                print("Request not found")
        '''    
    except Exception as e:
        print(f"Error acessing match {id_match}: {e}")

driver.quit()