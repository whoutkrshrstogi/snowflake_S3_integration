import boto3
import datetime


class S3:

    def get_s3_client(s3_config : dict):
        return boto3.client(**s3_config)
    

    def get_s3_last_modified(s3 : boto3.client, bucket_name, s3_key):
        try:
            response = s3.head_object(Bucket=bucket_name, Key=s3_key)
            return response['LastModified']
        except s3.exceptions.ClientError:
            return None
    

    def verify_lates_file(previous_time : datetime, s3_latest : datetime) -> bool:
        if previous_time <= s3_latest:
            print("latest file recieved")
            return True
        else:
            print("No new file recieved")
            return False
        