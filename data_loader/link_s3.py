"""
this file should provide a class for the user to load files
in given AWS S3 link
"""

import os
import sagemaker
import boto3

import logging

from botocore.exceptions import ClientError


class AWS_S3_Loader:
    AWS_DEFAULT_REGION="ap-southeast-1"
    AWS_ACCESS_KEY_ID= "ASIA6HN7YGSV6NGLNAJR"
    AWS_SECRET_ACCESS_KEY="/B0IUnQhP2AhxYvGiXFeMu0aY6LMPM/acaVWx5yC"
    SESSION_TOKEN="IQoJb3JpZ2luX2VjEM3//////////wEaCXVzLWVhc3QtMSJHMEUCIQDutumRKqOXa7SiVVVUcGSMMTU+SRNDD39bkHjpsnSYGQIgMPl2i4Pgdbt07XHmyeezbETvNDFogYf8LOXPEj25JMAqoAIIhf//////////ARAAGgw5NzgwNDQwNzMxMzEiDO6cAFo/OubWKVPpByr0AbKFjXp+hS2lh1wQp8EJYgRNpfKQSQ16zu9jhRcBd4yJl8D/EZ7lnV/fdEa1hnDjXqdJtHbDf1SlvWTL6QZkoKToGn3IkqqEF34gFt4xFRFkybDfEfnffaaXw62sEm7buQ+iDlaAbnTsgrmYLyrDfbVeo79dBJCk/mJnZxh6BOq2tYDKMKD+B09xEA+gJwdf4biTAjWzeR1zBVpCpqobrFjRbF4W/bpAs6UAc3AgKs+MFpVd9IU8fJJmpL/lZVuloCF+nu9XEFMJyb+hHm3Yw1puV1aG0IdDapH/UfFRQAX5WdoMRSMhd3uYUl7FWPSjSwxK5sowuv+voAY6nQHb+J25voauxka2rn/PVi8w40RjmaR7ElS7y045qWs5io0sU1BBBkWqN49gBf9JuJvw40Cu7AHvabRxWlTBP0ar/qo5Smw9V0a1z4irF3/AtIx837v6JCK4cfyhHahFD3rNTP0cT77ZYbHjnfYpV8b4im4cpWGqeK8rQYkpAk5dVMMP5dYHWce2+YnAyluG1QpSvM+5A/KCU37oKncm"


    def __init__(self, bucket_name):
        self.client = boto3.client('s3', 
                    region_name=self.AWS_DEFAULT_REGION, 
                    aws_access_key_id=self.AWS_ACCESS_KEY_ID, 
                    aws_secret_access_key=self.AWS_SECRET_ACCESS_KEY, 
                    aws_session_token=self.SESSION_TOKEN)
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

    def download_file(self, file_path, local_path):
        try:
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
