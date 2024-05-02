# Import modules
import json
import os
import requests
import boto3
from dotenv import load_dotenv

# Import environment variables
load_dotenv()

# S3 Bucket Class for storage + retrieval of extracted content
class S3:
    # Define class variables
    access_key_id = os.environ.get('AWS_ACCESS_KEY_ID')
    secret_access_key = os.environ.get('AWS_SECRET_ACCESS_KEY')
    bucket = os.environ.get('S3_BUCKET')

    # Connect to instance of s3 bucket
    @classmethod
    def connect(cls):
        cls.s3_client=boto3.client('s3', aws_access_key_id=cls.access_key_id, aws_secret_access_key=cls.secret_access_key)

    # Disconnect from instance of s3 bucket
    @classmethod
    def disconnect(cls):
        del cls.s3_client
    
    # Extract data from specified object
    @classmethod
    def retreive(cls, folder: str, object_key: str):
        cls.connect()
        object = cls.s3_client.get_object(Bucket=cls.bucket, Key=folder + object_key)
        contents = object['Body'].read().decode('utf-8')
        yield json.loads(contents)
        cls.disconnect()

    # Store data into s3 bucket
    @classmethod
    def store(cls, data: list, folder: str, object_key: str):
        data_json = json.dumps(data)
        cls.connect()
        cls.s3_client.put_object(Bucket=cls.bucket, Key=folder + object_key, Body=data_json, ContentType='application/json')
        cls.disconnect()
    










    
