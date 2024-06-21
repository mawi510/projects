#!/usr/bin/env python
# coding: utf-8

# In[13]:


'''
In this notebook, we'll practice writing to our S3 bucket, and reading a file from said S3 bucket
Let's import our packages
'''

import pandas as pd
import boto3


# In[14]:


#Let's pull our S3 Credentials, and specify the S3 bucket we wish to write to

get_ipython().run_line_magic('store', '-r aws_access_key_id')
get_ipython().run_line_magic('store', '-r aws_secret_access_key')

aws_s3_bucket = "latestnfldata"


# In[15]:


#Let's preview our csv file that we want to send to S3

df = pd.read_csv('nfl_training_data_2023.csv')
df.head()


# In[16]:


#Now we can use pandas to write this file to S3

df.to_csv(
    f"s3://{aws_s3_bucket}/nfl_training_data_2023.csv",
    index=False,
    storage_options={
        "key": aws_access_key_id,
        "secret": aws_secret_access_key,
        }
)


# In[17]:


#Upload was successful


# In[19]:


#Now we can read the file from the S3 bucket as such


df = pd.read_csv(
    f"s3://{aws_s3_bucket}/nfl_training_data_2023.csv",
    storage_options={
        "key": aws_access_key_id,
        "secret": aws_secret_access_key,
        }
)

df.head()


# In[ ]:




