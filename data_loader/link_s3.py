"""
this file should provide a class for the user to load files
in given AWS S3 link
"""

import os
import sagemaker
import boto3
import sys

import logging
# import tqdm
from botocore.exceptions import ClientError


class AWS_S3_Loader:
    AWS_DEFAULT_REGION=os.getenv('AWS_DEFAULT_REGION')
    AWS_ACCESS_KEY_ID=os.getenv('AWS_ACCESS_KEY_ID')
    AWS_SECRET_ACCESS_KEY=os.getenv('AWS_SECRET_ACCESS_KEY')
    SESSION_TOKEN=os.getenv('AWS_SESSION_TOKEN')

    if not all ([AWS_DEFAULT_REGION, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, SESSION_TOKEN]):
        
        print("Please set the environment variables")
        sys.exit()

    logging.info("AWS_DEFAULT_REGION: %s", AWS_DEFAULT_REGION)
    logging.info("AWS_ACCESS_KEY_ID: %s", AWS_ACCESS_KEY_ID)
    logging.info("AWS_SECRET_ACCESS_KEY: %s", AWS_SECRET_ACCESS_KEY)
    logging.info("SESSION_TOKEN: %s", SESSION_TOKEN[:10])

    def __init__(self, bucket_name):
        self.client = boto3.client('s3', 
                    region_name=self.AWS_DEFAULT_REGION, 
                    aws_access_key_id=self.AWS_ACCESS_KEY_ID, 
                    aws_secret_access_key=self.AWS_SECRET_ACCESS_KEY, 
                    aws_session_token=self.SESSION_TOKEN
                    )
        self.bucket_name = bucket_name

    def list_files(self, bucket_name, path):
        response = self.client.list_objects_v2(Bucket=self.bucket_name, Prefix=path)
        return [content['Key'] for content in response['Contents']]
        

    def upload_file(self, file_name,object_name=None):
        """Upload a file to an S3 bucket

        :param file_name: File to upload
        :param bucket: Bucket to upload to
        :param object_name: S3 object name. If not specified then file_name is used
        :return: True if file was uploaded, else False
        """

        # If S3 object_name was not specified, use file_name
        if object_name is None:
            object_name = os.path.basename(file_name)

        # Upload the file
        s3_client = self.client
        try:
            response = s3_client.upload_file(file_name, self.bucket_name, object_name)
        except ClientError as e:
            logging.error(e)
        return True
    
    def list_files(self, bucket_name, path):
        response = self.client.list_objects_v2(Bucket=self.bucket_name, Prefix=path)
        return [content['Key'] for content in response['Contents']]

    def download_dir(self, remote_path, local_path):
        if not os.path.exists(local_path):
            os.makedirs(local_path)
        for file in self.list_files(self.bucket_name, remote_path):
            self.download_file(file, file.replace(remote_path, local_path, 1))


    def download_file(self, file_path, local_path):
        try:
            if not os.path.exists(os.path.dirname(local_path)):
                os.makedirs(os.path.dirname(local_path))

            if os.path.exists(local_path):
                if input(f"File {local_path} already exists. Do you want to overwrite it? (y/n)") != "y":
                    return False
            response=self.client.download_file(self.bucket_name, file_path, local_path)
            
        except ClientError as e:
            logging.error(e)
            return False
        return True
    
    def CreateBucket(self, bucket_name):
        """Create an S3 bucket in a specified region

        If a region is not specified, the bucket is created in the S3 default
        region (us-east-1).

        :param self.bucket_name: Bucket to create
        :param region: String region to create bucket in, e.g., 'us-west-2'
        :return: True if bucket created, else False
        """

        # Create bucket
        try:
                self.client.create_bucket(Bucket=bucket_name)

        except ClientError as e:
            logging.error(e)
            return None
        return AWS_S3_Loader(bucket_name)
