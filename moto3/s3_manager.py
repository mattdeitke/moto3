import logging
import os
import shutil
from datetime import datetime, timedelta
from typing import Optional
from tenacity import retry, stop_after_attempt, wait_exponential
from botocore.exceptions import ClientError

import boto3
from tqdm import tqdm

# Configure the logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

handler = logging.StreamHandler()
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
handler.setFormatter(formatter)
logger.addHandler(handler)

region_name = "us-west-2"
s3_client = boto3.client("s3")
s3_resource = boto3.resource("s3")


class S3Manager:
    def __init__(self, bucket_name: str):
        """Create a new S3Manager instance.

        If the bucket does not exist, it will be created.

        Args:
            bucket_name (str): The name of the bucket to use, which may
                include a directory path (e.g. "my-bucket/my-dir").
                In such a case, only my-bucket will be created and
                my-dir will be used as the directory path for all
                operations.
        """
        if "/" in bucket_name:
            self.bucket_name = bucket_name.split("/")[0]
            self.dirpath = bucket_name[len(bucket_name) + 1 :]
        else:
            self.bucket_name = bucket_name
            self.dirpath = ""
        S3Manager.create_bucket(self.bucket_name)

    @staticmethod
    def create_bucket(bucket_name: str) -> None:
        try:
            s3_resource.meta.client.head_bucket(Bucket=bucket_name)
            logger.info(f"Bucket '{bucket_name}' found.")
        except s3_client.exceptions.ClientError:
            logger.info(f"Bucket '{bucket_name}' not found. Creating a new bucket.")
            s3_client.create_bucket(
                Bucket=bucket_name,
                CreateBucketConfiguration={"LocationConstraint": region_name},
            )
            logger.info(f"Bucket '{bucket_name}' created successfully.")
        except s3_client.exceptions.BucketAlreadyOwnedByYou:
            logger.info(f"Bucket '{bucket_name}' found.")

    @staticmethod
    def list_buckets() -> list:
        response = s3_client.list_buckets()
        sorted_buckets = sorted(
            response["Buckets"], key=lambda bucket: bucket["CreationDate"], reverse=True
        )
        return [bucket["Name"] for bucket in sorted_buckets]

    @retry(
        stop=stop_after_attempt(5), wait=wait_exponential(multiplier=1, min=4, max=10)
    )
    def upload(self, obj: str, key: str) -> None:
        file_path = os.path.join(self.dirpath, key)
        try:
            s3_client.put_object(Bucket=self.bucket_name, Key=file_path, Body=obj)
        except ClientError as e:
            if e.response["Error"]["Code"] == "SlowDown":
                print("Exceeded request rate. Retrying...")
                raise
            else:
                raise  # re-throw the last exception if SlowDown was not the cause

    def delete(self, key: str) -> None:
        file_path = os.path.join(self.dirpath, key)
        s3_client.delete_object(Bucket=self.bucket_name, Key=file_path)

    def upload_file(self, file_path: str, key: str) -> None:
        """Upload a file to S3.

        Args:
            file_path (str): The path to the file to upload.
            key (str): The key to use for the file in S3.
        """
        upload_path = os.path.join(self.dirpath, key)
        s3_client.upload_file(file_path, self.bucket_name, upload_path)

    def read_file(self, key: str, decode: Optional[str] = "utf-8") -> str:
        """Read a file from S3.

        Args:
            key (str): The key to use for the file in S3.
            decode (str, optional): The type of decoding to use. Defaults to "utf-8".

        Returns:
            str: The contents of the file.
        """
        s3_file_path = os.path.join(self.dirpath, key)
        obj = s3_resource.Object(self.bucket_name, s3_file_path)
        obj = obj.get()["Body"].read()
        if decode == "utf-8":
            obj = obj.decode("utf-8")
        elif decode is None:
            pass
        else:
            raise ValueError(f"Unsupported decode type: {decode}")
        return obj

    def exists(self, key: str) -> bool:
        """Check if a file exists in S3.

        Args:
            key (str): The key to use for the file in S3.

        Returns:
            bool: True if the file exists, False otherwise.
        """
        s3_file_path = os.path.join(self.dirpath, key)
        results = s3_client.list_objects(Bucket=self.bucket_name, Prefix=s3_file_path)
        return "Contents" in results

    def download_file(self, key: str, file_path: str) -> None:
        s3_file_path = os.path.join(self.dirpath, key)
        s3_client.download_file(self.bucket_name, s3_file_path, file_path)

    @retry(
        stop=stop_after_attempt(5), wait=wait_exponential(multiplier=1, min=4, max=10)
    )
    def list_all_files(
        self,
        prefix: str = "",
        max_files: Optional[int] = None,
        show_progress: bool = True,
        last_modified_hours: Optional[int] = None,
    ) -> list:
        bucket = s3_resource.Bucket(self.bucket_name)
        prefix_path = os.path.join(self.dirpath, prefix)
        filtered_objects = bucket.objects.filter(Prefix=prefix_path)

        if last_modified_hours is not None:
            first_obj_tz = next(iter(filtered_objects)).last_modified.tzinfo
            min_last_modified_date = datetime.now(first_obj_tz) - timedelta(
                hours=last_modified_hours
            )
            filtered_objects = [
                obj
                for obj in filtered_objects
                if obj.last_modified > min_last_modified_date
            ]

        if max_files is None:
            out_iter = tqdm(filtered_objects) if show_progress else filtered_objects
            return [obj.key for obj in out_iter]
        else:
            out_iter = zip(range(max_files), filtered_objects)
            if show_progress:
                out_iter = tqdm(out_iter, total=max_files)
            return [obj.key for _, obj in out_iter]

    def get_file_count(self, days_ago: int = 5):
        """Get the number of files in the bucket for each day in the past.

        Warning - this method will ignore the directory path if one was
        specified when creating the S3Manager instance.
        """
        cloudwatch = boto3.client("cloudwatch")
        daily_counts = []

        for i in range(days_ago, 0, -1):
            start_time = datetime.now() - timedelta(days=i)
            end_time = start_time + timedelta(days=1)
            response = cloudwatch.get_metric_statistics(
                Namespace="AWS/S3",
                MetricName="NumberOfObjects",
                Dimensions=[
                    {"Name": "BucketName", "Value": self.bucket_name},
                    {"Name": "StorageType", "Value": "AllStorageTypes"},
                ],
                StartTime=start_time,
                EndTime=end_time,
                Period=86400,  # 24 hours in seconds
                Statistics=["Average"],
            )

            if response["Datapoints"]:
                total_objects = response["Datapoints"][0]["Average"]
                daily_counts.append((start_time.date(), total_objects))

        # Sort the list by date in descending order (latest counts first)
        daily_counts.sort(key=lambda x: x[0], reverse=True)

        # Format the list with dates as "MM-DD-YYYY"
        formatted_daily_counts = [
            (date.strftime("%m-%d-%Y"), int(count)) for date, count in daily_counts
        ]

        return formatted_daily_counts


class LocalStorageManager:
    def __init__(self, root_dir: str):
        self.root_dir = root_dir

    @staticmethod
    def list_buckets() -> list:
        raise Exception("LocalStorageManager does not support list_buckets()")

    def upload(self, obj: str, key: str) -> None:
        # make the key directory
        obj_dir = os.path.join(self.root_dir, os.path.dirname(key))
        os.makedirs(obj_dir, exist_ok=True)

        # write the object to the key
        obj_path = os.path.join(self.root_dir, key)
        with open(obj_path, "w") as f:
            f.write(obj)

    def delete(self, key: str) -> None:
        obj_path = os.path.join(self.root_dir, key)
        os.remove(obj_path)

    def upload_file(self, file_path: str, key: str) -> None:
        # make the key directory
        obj_dir = os.path.join(self.root_dir, os.path.dirname(key))
        os.makedirs(obj_dir, exist_ok=True)

        # copy the file to the key
        obj_path = os.path.join(self.root_dir, key)
        shutil.copy(file_path, obj_path)

    def read_file(self, key: str) -> str:
        obj_path = os.path.join(self.root_dir, key)
        with open(obj_path, "r") as f:
            return f.read()

    def exists(self, key: str) -> bool:
        obj_path = os.path.join(self.root_dir, key)
        return os.path.exists(obj_path)

    def list_all_files(
        self,
        prefix: str = "",
        max_files: Optional[int] = None,
        show_progress: bool = True,
    ) -> list:
        dir_to_search = os.path.join(self.root_dir, prefix)
        file_list = []
        for root, dirs, files in os.walk(dir_to_search):
            for file in files:
                file_list.append(os.path.join(root, file))
                if max_files is not None and len(file_list) >= max_files:
                    return file_list
        return file_list

    def get_file_count(self, days_ago: int = 5):
        return len(self.list_all_files())
