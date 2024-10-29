import boto3
import re
import os
from dotenv import load_dotenv 
import sys
import argparse

load_dotenv()

aws_access_key_id = os.getenv("AWS_ACCESS_KEY_ID")
aws_secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY")
aws_default_region = os.getenv("AWS_DEFAULT_REGION")

print("AWS_ACCESS_KEY_ID:", aws_access_key_id)
print("AWS_SECRET_ACCESS_KEY:", aws_secret_access_key)
print("AWS_DEFAULT_REGION:", aws_default_region)

s3 = boto3.client(
    's3',
    aws_access_key_id=aws_access_key_id,
    aws_secret_access_key=aws_secret_access_key,
    region_name=aws_default_region
)

def list_files(bucket_name):
    """Lists"""
    try:
        objects = s3.list_objects_v2(Bucket=bucket_name, Prefix="b-wing/")
        if 'Contents' in objects:
            for obj in objects['Contents']:
                print(obj['Key'])
        else:
            print("No files found in the specified prefix or bucket is empty.")
    except Exception as e:
        print(f"Error listing files: {e}")

def upload_file(bucket_name, file_path, target_key):
    """Uploads"""
    try:
        s3.upload_file(file_path, bucket_name, target_key)
        print(f"File {file_path} uploaded as {target_key} in bucket {bucket_name}.")
    except Exception as e:
        print(f"Error uploading file: {e}")

def list_filtered_files(bucket_name, filter_regex):
    """Lists files in the bucket matching a given regular expression"""
    try:
        objects = s3.list_objects_v2(Bucket=bucket_name, Prefix="b-wing/")
        pattern = re.compile(filter_regex)
        if 'Contents' in objects:
            for obj in objects['Contents']:
                if pattern.search(obj['Key']):
                    print(obj['Key'])
        else:
            print("The bucket is empty or does not exist.")
    except Exception as e:
        print(f"Error filtering files: {e}")

def delete_filtered_files(bucket_name, filter_regex):
    """Delete"""
    try:
        objects = s3.list_objects_v2(Bucket=bucket_name, Prefix="b-wing/")
        pattern = re.compile(filter_regex)
        if 'Contents' in objects:
            for obj in objects['Contents']:
                if pattern.search(obj['Key']):
                    s3.delete_object(Bucket=bucket_name, Key=obj['Key'])
                    print(f"Deleted file: {obj['Key']}")
        else:
            print("No files found matching the filter.")
    except Exception as e:
        print(f"Error deleting files: {e}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="CLI to manage files in AWS S3")
    parser.add_argument("operation", choices=["list", "upload", "list-filtered", "delete-filtered"], help="Type of operation to perform")
    parser.add_argument("--bucket", required=True, help="S3 bucket name")
    parser.add_argument("--file", help="Path to the local file (for upload)")
    parser.add_argument("--target", help="Target location in the bucket (for upload)")
    parser.add_argument("--filter", help="Regular expression for filtering files")

    args = parser.parse_args()

    if args.operation == "list":
        list_files(args.bucket)
    elif args.operation == "upload" and args.file and args.target:
        upload_file(args.bucket, args.file, args.target)
    elif args.operation == "list-filtered" and args.filter:
        list_filtered_files(args.bucket, args.filter)
    elif args.operation == "delete-filtered" and args.filter:
        delete_filtered_files(args.bucket, args.filter)
    else:
        print("Invalid arguments.")
