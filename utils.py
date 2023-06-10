import os
import gspread
import pandas as pd
from oauth2client.service_account import ServiceAccountCredentials
# Write code to automatically save df to s3 bucket
import boto3
from botocore.exceptions import ClientError
import os
from io import StringIO
import pandas as pd
from dotenv import load_dotenv
load_dotenv()

def get_sheet_data(url, secret_file, filename=None, save_as_csv=False):
    scope = ['https://spreadsheets.google.com/feeds',
                'https://www.googleapis.com/auth/drive']
    creds = ServiceAccountCredentials.from_json_keyfile_name(secret_file, scope)
    client = gspread.authorize(creds)
    spreadsheet = client.open_by_url(url)
    sheet = spreadsheet.sheet1
    data = sheet.get_all_values()
    df = pd.DataFrame(data[1:], columns=data[0])
    if save_as_csv:
        df.to_csv(filename)
    return df

def upload_df(df, object_name, ACCESS_KEY, SECRET_KEY, bucket='crimepred-data'):
    """Upload a file to an S3 bucket
    :param df: DataFrame to upload
    :param bucket: Bucket to upload to
    :param object_name: S3 object name. If not specified then file_name is used
    """
    client = boto3.client("s3", region_name='us-east-2', aws_access_key_id=ACCESS_KEY,
                          aws_secret_access_key=SECRET_KEY)
    csv_buf = StringIO()
    df.to_csv(csv_buf, header=True, index=False)
    csv_buf.seek(0)
    client.put_object(Bucket=bucket, Body=csv_buf.getvalue(), Key=object_name)
    print(
        f'Copied {df.shape[0]} rows to S3 Bucket {bucket} at {object_name}, Done!')


def download_df(object_name, ACCESS_KEY, SECRET_KEY, bucket='crimepred-data'):
    """Download a file from an S3 bucket
    :param object_name: S3 object name. If not specified then file_name is used
    :param bucket: Bucket to download from
    """
    client = boto3.client("s3", region_name='us-east-2', aws_access_key_id=ACCESS_KEY,
                          aws_secret_access_key=SECRET_KEY)
    try:
        obj = client.get_object(Bucket=bucket, Key=object_name)
        df = pd.read_csv(obj['Body'])
        print(f'Downloaded {df.shape[0]} rows from S3 Bucket {bucket} at {object_name}, Done!')
        return df
    except ClientError as e:
        if e.response['Error']['Code'] == "404":
            print("The object does not exist.")
        else:
            print(f" Unexpected error: {e}")
    return None
