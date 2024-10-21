''' Import modules '''
import os
from dotenv import load_dotenv

# Import environment variables
load_dotenv()

class Config:
    ''' Class which initialises environment variables used by S3, S3Metadata, EIA and NOAA classes '''
    def __init__(self):
        self.access_key_id = os.environ.get('AWS_ACCESS_KEY_ID')
        self.secret_access_key = os.environ.get('AWS_SECRET_ACCESS_KEY')
        self.bucket = os.environ.get('S3_BUCKET')
        self.eia_api_key = os.environ.get('API_KEY')
        self.token = os.environ.get('TOKEN')