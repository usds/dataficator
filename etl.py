import boto3
import pandas as pd
import os

CLOUDFLARE_ACCOUNT_ID = os.environ.get("CLOUDFLARE_ACCOUNT_ID")
AWS_ACCESS_KEY_ID = os.environ.get("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.environ.get("AWS_SECRET_ACCESS_KEY")

s3_bucket_name = 'incoming'
s3 = boto3.resource('s3',
                    endpoint_url = f"https://{CLOUDFLARE_ACCOUNT_ID}.r2.cloudflarestorage.com",
                    aws_access_key_id = AWS_ACCESS_KEY_ID,
                    aws_secret_access_key = AWS_SECRET_ACCESS_KEY)

cvses_remote_object = s3.Object(s3_bucket_name,'cvses.csv')
cvses_response = object.get()
cvses_df = pd.read_csv(cvses_response['Body'], dtype='string')

print(cvses_df.head())
