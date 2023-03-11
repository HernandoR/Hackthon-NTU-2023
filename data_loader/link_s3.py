"""
this file should provide a class for the user to load files
in given AWS S3 link
"""

import sagemaker

class AWS_S3_Loader:
    def __init__(self, bucket_name, file_path):
        self.bucket_name = bucket_name
        self.bucket_link = 's3://' + bucket_name
        self.file_path = file_path

    def load(self):
        s3 = sagemaker.Session().boto_session.resource('s3')
        return s3.Object(self.bucket_name, self.file_path).get()['Body'].read().decode('utf-8')