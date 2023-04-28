import boto3
import json
from multiprocessing import Pool
import logging
from tqdm import tqdm
import multiprocessing

# Configure the logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

handler = logging.StreamHandler()
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
handler.setFormatter(formatter)
logger.addHandler(handler)


class QueueManager:
    def __init__(self, queue_name: str) -> None:
        self.sqs = boto3.resource("sqs")
        self.queue_name = queue_name

        try:
            self.queue = self.sqs.get_queue_by_name(QueueName=self.queue_name)
            logger.info(f"Queue '{self.queue_name}' found.")
        except self.sqs.meta.client.exceptions.QueueDoesNotExist:
            logger.warning(
                f"Queue '{self.queue_name}' not found. Creating a new queue."
            )
            self.queue = self.sqs.create_queue(QueueName=self.queue_name)
            logger.info(f"Queue '{self.queue_name}' created successfully.")

        self.queue_url = self.queue.url

    @property
    def size(self) -> int:
        return self.queue.attributes["ApproximateNumberOfMessages"]

    @staticmethod
    def _upload_batch(x) -> None:
        sqs, batch, queue_url = x
        sqs.send_message_batch(QueueUrl=queue_url, Entries=batch)

    def upload(self, messages: list) -> None:
        messages = [
            {
                "Id": str(i),
                "MessageBody": message
                if isinstance(message, str)
                else json.dumps(message),
            }
            for i, message in enumerate(messages)
        ]
        batch_size = 10
        batches = [
            (self.sqs, messages[i : i + batch_size], self.queue_url)
            for i in range(0, len(messages), batch_size)
        ]
        # use tqdm and multiprocessing to upload the batches
        with Pool(multiprocessing.cpu_count()) as p:
            list(tqdm(p.imap(self._upload_batch, batches), total=len(batches)))

    def get_next(self, max_messages: int = 1) -> list:
        response = self.queue.receive_messages(MaxNumberOfMessages=max_messages)
        message = response["Messages"][0]
        item = json.loads(message.body)
        return message, item

    def delete(self, message) -> None:
        self.queue.delete_message(ReceiptHandle=message["ReceiptHandle"])
