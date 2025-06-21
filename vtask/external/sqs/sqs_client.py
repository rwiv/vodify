import asyncio

from aiobotocore.session import get_session
from pydantic import BaseModel, constr
from types_aiobotocore_sqs.client import SQSClient
from types_aiobotocore_sqs.type_defs import MessageTypeDef


class SQSConfig(BaseModel):
    access_key: constr(min_length=1)
    secret_key: constr(min_length=1)
    region_name: constr(min_length=1)
    queue_url: constr(min_length=1)


class SQSAsyncClient:
    def __init__(self, conf: SQSConfig):
        self.__conf = conf

    async def send(self, body: str):
        async with create_client(self.__conf) as client:
            await client.send_message(QueueUrl=self.__conf.queue_url, MessageBody=body)

    async def receive(self, wait_time_sec: int = 20, max_num: int = 10) -> list[str | None]:
        async with create_client(self.__conf) as client:
            response = await client.receive_message(
                QueueUrl=self.__conf.queue_url,
                WaitTimeSeconds=wait_time_sec,
                MaxNumberOfMessages=max_num,
            )
            messages: list[MessageTypeDef] = response.get("Messages", [])

            result: list[str | None] = []
            for message in messages:
                result.append(message.get("Body"))

            coroutines = []
            for message in messages:
                handle = message.get("ReceiptHandle")
                if handle is not None:
                    coroutines.append(client.delete_message(QueueUrl=self.__conf.queue_url, ReceiptHandle=handle))
            await asyncio.gather(*coroutines)
            return result


def create_client(conf: SQSConfig) -> SQSClient:
    client = get_session().create_client(
        "sqs",
        region_name=conf.region_name,
        aws_access_key_id=conf.access_key,
        aws_secret_access_key=conf.secret_key,
    )
    return client  # type: ignore
