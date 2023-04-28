import boto3
import logging

# Configure the logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

handler = logging.StreamHandler()
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
handler.setFormatter(formatter)
logger.addHandler(handler)

s3_client = boto3.client("s3")
s3_resource = boto3.resource("s3")


class S3Manager:
    def __init__(self, bucket_name: str):
        self.bucket_name = bucket_name
        self._create_bucket(bucket_name)

    def _create_bucket(self, bucket_name: str) -> None:
        try:
            s3_resource.meta.client.head_bucket(Bucket=bucket_name)
            print(f"Bucket '{bucket_name}' found.")
        except s3_client.exceptions.NoSuchBucket:
            print(f"Bucket '{bucket_name}' not found. Creating a new bucket.")
            s3_client.create_bucket(Bucket=bucket_name)
            print(f"Bucket '{bucket_name}' created successfully.")

    def upload(self, obj: str, key: str) -> None:
        s3_client.put_object(Bucket=self.bucket_name, Key=key, Body=obj)

    def upload_file(self, file_path: str, key: str) -> None:
        s3_client.upload_file(file_path, self.bucket_name, key)

    def read_file(self, key: str) -> str:
        obj = s3_resource.Object(self.bucket_name, key)
        return obj.get()["Body"].read().decode("utf-8")

    def exists(self, key: str) -> bool:
        try:
            s3_resource.meta.client.head_object(Bucket=self.bucket_name, Key=key)
            return True
        except s3_client.exceptions.NoSuchKey:
            return False

    def list_all_files(bucket_name: str) -> list:
        bucket = s3_resource.Bucket(bucket_name)
        return [obj.key for obj in bucket.objects.all()]
