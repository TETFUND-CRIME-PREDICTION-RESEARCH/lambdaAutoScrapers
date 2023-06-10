from utils import *
from dotenv import load_dotenv
load_dotenv()
import os
from datetime import datetime

# get env variables
ACCESS_KEY = os.getenv("AWS_ACCESS_KEY_ID") 
SECRET_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")  
DATA_URL = os.getenv("DATA_URL")
KEY_FILE = os.getenv("KEY_FILE")

def main_handler(event, context):
    df = get_sheet_data(DATA_URL, KEY_FILE, save_as_csv=True)
    upload_df(df, 'recent_crime_data.csv', ACCESS_KEY, SECRET_KEY)
    return {
        'statusCode': 200,
        'body': f"Successfully uploaded {df.shape[0]} rows to S3 Bucket crimepred-data at recent_crime_data.csv at {datetime.now()}"
    }

if __name__ == "__main__":
    main_handler(None, None)
    