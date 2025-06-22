from aiobotocore.session import get_session
from pydantic import BaseModel, constr
from types_aiobotocore_sqs.client import SQSClient
from types_aiobotocore_sqs.type_defs import MessageTypeDef, DeleteMessageBatchRequestEntryTypeDef


class SQSConfig(BaseModel):
    access_key: constr(min_length=1)
    secret_key: constr(min_length=1)
    region_name: constr(min_length=1)
    queue_url: constr(min_length=1)


class SQSMessage(BaseModel):
    id: str
    body: str
    handle: str


class SQSAsyncClient:
    def __init__(self, conf: SQSConfig):
        self.__conf = conf

    async def send(self, body: str):
        async with create_client(self.__conf) as client:
            await client.send_message(QueueUrl=self.__conf.queue_url, MessageBody=body)

    async def receive(self, wait_time_sec: int = 20, max_num: int = 10) -> list[SQSMessage]:
        async with create_client(self.__conf) as client:
            response = await client.receive_message(
                QueueUrl=self.__conf.queue_url,
                WaitTimeSeconds=wait_time_sec,
                MaxNumberOfMessages=max_num,
            )
            messages: list[MessageTypeDef] = response.get("Messages", [])
            result: list[SQSMessage] = []
            for msg in messages:
                msg_id = msg.get("MessageId")
                body = msg.get("Body")
                handle = msg.get("ReceiptHandle")
                if not msg_id or not body or not handle:
                    raise ValueError(f"Received message with missing fields: {msg}")
                result.append(SQSMessage(id=msg_id, body=body, handle=handle))
            return result

    async def delete(self, messages: list[SQSMessage]):
        if len(messages) == 0:
            return
        async with create_client(self.__conf) as client:
            if len(messages) == 1:
                await client.delete_message(QueueUrl=self.__conf.queue_url, ReceiptHandle=messages[0].handle)
            else:
                entries: list[DeleteMessageBatchRequestEntryTypeDef] = []
                for msg in messages:
                    entries.append({"Id": msg.id, "ReceiptHandle": msg.handle})
                await client.delete_message_batch(QueueUrl=self.__conf.queue_url, Entries=entries)


def create_client(conf: SQSConfig) -> SQSClient:
    client = get_session().create_client(
        "sqs",
        region_name=conf.region_name,
        aws_access_key_id=conf.access_key,
        aws_secret_access_key=conf.secret_key,
    )
    return client  # type: ignore
