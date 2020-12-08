#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Jun 24 13:07:54 2020

@author: praveenkumar
"""
import hashlib
import random
import boto3

access_key = 'l0QlE6PqxzztWgdQ7gSq'
access_secret = 'tFqkwZQrC+6hQHNtl05pfxpRwN4VYmF90xrM6AjC'
bucket_name = 'fci-analytics-136ed86b-e58d-4cf0-8aea-e46886a73296'
filename = 'party.csv'
object_key = 'feature/' + filename
path = '/Users/praveenkumar/fci/fci-analytics-runtime/explore/'
endpoint = 'https://s3-openshift-storage.apps.ssk.os.fyre.ibm.com:443'


def upload_file(Client, Bucket, Key, Filename):
    config = boto3.s3.transfer.TransferConfig(
        # number of bytes   default: multipart_threshold=8388608 --> 8mb
        multipart_threshold=8388608,
        # The partition size of each part for a multipart transfer.
        multipart_chunksize=8388608,
        max_concurrency=10,
        num_download_attempts=10,
        use_threads=False
    )
    transfer = boto3.s3.transfer.S3Transfer(Client, config)
    transfer.upload_file(Filename, Bucket, Key)


def get_existing_blog_size(client, bucket, key):
    response = client.list_objects_v2(Bucket=bucket, Prefix=key)
    for obj in response.get('Contents', []):
        if obj['Key'] == key:
            return obj['Size']


UL_client = boto3.client(
    's3',
    aws_access_key_id=access_key,
    aws_secret_access_key=access_secret,
    region_name=bucket_name,
    endpoint_url=endpoint,
    verify=False,
    use_ssl=True
)


transfer = boto3.s3.transfer.S3Transfer(UL_client)
# response =
transfer.upload_file(path + filename, bucket_name, object_key)

# First upload test
response = UL_client.upload_file(
    path + filename, bucket_name, object_key, ExtraArgs={'Tagging': 'type=test'})
size = get_existing_blog_size(UL_client, bucket_name, object_key)
print(size)

response = UL_client.delete_object(
    Bucket=bucket_name,
    Key=object_key,
)
print("Delete response", response)

size = get_existing_blog_size(UL_client, bucket_name, object_key)
print(size)


print("Single PUT")
# Second upload test
response = UL_client.put_object(
    Body="sloth",
    Bucket=bucket_name,
    Key=object_key
)

print("Response", response)