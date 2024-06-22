#Let's import our requirements

import pandas as pd
import boto3
import os


#Let's pull our S3 Credentials, and specify the S3 bucket we wish to write to

aws_access_key_id = os.getenv('AWS_ACCESS_KEY_ID')
aws_secret_access_key = os.getenv('AWS_SECRET_ACCESS_KEY')

aws_s3_bucket = "latestnfldata"


#Let's pull our csv
df = pd.read_csv('PredictingNFLGames/nfl_training_data_2023.csv')

#Now we can use pandas to write this file to S3

df.to_csv(
    f"s3://{aws_s3_bucket}/nfl_training_data_2023.csv",
    index=False,
    storage_options={
        "key": aws_access_key_id,
        "secret": aws_secret_access_key,
        }
)



