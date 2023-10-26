import boto3

from configparser import ConfigParser
import pandas as pd
import redshift_connector as rdc

config = ConfigParser()

config.read('.env')

ACCESS_KEY = config['AWS']['access_key']
SECRET_KEY = config['AWS']['secret_key']
BUCKET_NAME = config['AWS']['bucket_name']
REGION = config['AWS']['region']

DWH_HOST = config['DWH']['host']
DWH_USER = config['DWH']['user']
DWH_DB = config['DWH']['database']
DWH_PASSWORD = config['DWH']['password']


RAW_DATASET = config['DWH']['raw_dataset']
STAGING_DATASET = config['DWH']['staging_dataset']


s3_lake_path = "s3://{}/{}.csv"


def create_s3_bucket():
    try:
        client = boto3.client(
            's3',
            aws_access_key_id=ACCESS_KEY,
            aws_secret_access_key=SECRET_KEY,
            region_name=REGION
        )
        client.create_bucket(
            Bucket=BUCKET_NAME,
            CreateBucketConfiguration={
                'LocationConstraint': REGION
            }
        )
        print('Bucket Created in S3lake')
    except Exception as error:
        print('Creation failed or Bucket exists')


def connect_to_warehouse():
    conn = rdc.connect(
        host=DWH_HOST, user=DWH_USER, password=DWH_PASSWORD, database=DWH_DB
    )
    print('Connected to DWH')
    return conn
