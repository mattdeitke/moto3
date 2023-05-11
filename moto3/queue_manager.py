import boto3
import time
import botocore
import json
from multiprocessing import Pool
import logging
from tqdm import tqdm
from typing import Any
import multiprocessing

# Configure the logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

handler = logging.StreamHandler()
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
handler.setFormatter(formatter)
logger.addHandler(handler)

sqs_client = boto3.client("sqs")
sqs_resource = boto3.resource("sqs")


def _upload_batch(args):
    batch, queue_url = args
    sqs_client.send_message_batch(QueueUrl=queue_url, Entries=batch)


def _upload_batch_with_retry(args: tuple) -> Any:
    batch, max_retries, sleep_time = args
    for retry in range(max_retries):
        try:
            _upload_batch(batch)
            return None  # If successful, return None
        except botocore.parsers.ResponseParserError as e:
            if retry < max_retries - 1:  # If not the last retry, sleep and retry
                time.sleep(sleep_time)
            else:  # If last retry, return the exception
                return e


class QueueManager:
    def __init__(self, queue_name: str) -> None:
        self.queue_name = queue_name
        self.queue_url = self._get_queue_url(queue_name)

    def _get_queue_url(self, queue_name):
        try:
            queue = sqs_resource.get_queue_by_name(QueueName=queue_name)
            logger.info(f"Queue '{queue_name}' found.")
        except sqs_resource.meta.client.exceptions.QueueDoesNotExist:
            logger.warning(f"Queue '{queue_name}' not found. Creating a new queue.")
            queue = sqs_resource.create_queue(QueueName=queue_name)
            logger.info(f"Queue '{queue_name}' created successfully.")
        return queue.url

    @property
    def size(self) -> int:
        queue = sqs_resource.Queue(self.queue_url)
        return int(queue.attributes["ApproximateNumberOfMessages"])

    def upload(self, messages: list, max_retries: int = 3, sleep_time: int = 5) -> None:
        messages = [
            {
                "Id": f"{i}",
                "MessageBody": (
                    message if isinstance(message, str) else json.dumps(message)
                ),
            }
            for i, message in enumerate(messages)
        ]
        batch_size = 10
        batches = [
            (messages[i : i + batch_size], self.queue_url)
            for i in range(0, len(messages), batch_size)
        ]
        if len(batches) < multiprocessing.cpu_count():
            logger.info("Uploading batches sequentially.")
            for batch in tqdm(batches):
                for retry in range(max_retries):
                    try:
                        _upload_batch(batch)
                        break  # If successful, break the retry loop
                    except botocore.parsers.ResponseParserError as e:
                        if (
                            retry < max_retries - 1
                        ):  # If not the last retry, sleep and retry
                            time.sleep(sleep_time)
                        else:  # If last retry, raise the exception
                            raise e
        else:
            # use tqdm and multiprocessing to upload the batches
            with Pool(multiprocessing.cpu_count()) as p:
                for result in tqdm(
                    p.imap(
                        _upload_batch_with_retry,
                        [(batch, max_retries, sleep_time) for batch in batches],
                    ),
                    total=len(batches),
                ):
                    if isinstance(
                        result, Exception
                    ):  # If an exception occurred in any worker, raise it
                        raise result

    def get_next(self, max_messages: int = 1, visibility_timeout: int = 30) -> list:
        queue = sqs_resource.Queue(self.queue_url)
        response = queue.receive_messages(
            MaxNumberOfMessages=max_messages,
            VisibilityTimeout=visibility_timeout
        )
        message = response[0]
        if message.body.startswith("{"):
            item = json.loads(message.body)
        else:
            item = message.body
        return message, item

    def delete(self, message) -> None:
        sqs_client.delete_message(
            QueueUrl=self.queue_url, ReceiptHandle=message.receipt_handle
        )

    def purge(self) -> None:
        sqs_client.purge_queue(QueueUrl=self.queue_url)
