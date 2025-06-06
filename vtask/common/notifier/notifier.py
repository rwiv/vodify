from abc import ABC, abstractmethod

import aiohttp
import requests
from pydantic import BaseModel
from pyutils import log

from ...common.env import UntfConfig


class Notifier(ABC):
    @abstractmethod
    def notify(self, message: str):  # TODO: remove
        pass

    @abstractmethod
    async def notify_async(self, message: str):  # TODO: rename to notify
        pass


class UntfSendRequest(BaseModel):
    topic: str
    message: str


class UntfNotifier(Notifier):
    def __init__(self, conf: UntfConfig):
        self.endpoint = conf.endpoint
        self.api_key = conf.api_key
        self.topic = conf.topic

    def notify(self, message: str) -> None:
        url = f"{self.endpoint}/api/send/v1"
        body = UntfSendRequest(topic=self.topic, message=message).model_dump(by_alias=True)
        headers = {"Content-Type": "application/json", "Authorization": f"Bearer {self.api_key}"}
        response = requests.post(url, json=body, headers=headers)
        if response.status_code >= 400:
            raise Exception(f"Failed to notify: {response.text}")

    async def notify_async(self, message: str):
        url = f"{self.endpoint}/api/send/v1"
        body = UntfSendRequest(topic=self.topic, message=message).model_dump(by_alias=True)
        headers = {"Content-Type": "application/json", "Authorization": f"Bearer {self.api_key}"}
        async with aiohttp.ClientSession(headers=headers) as session:
            async with session.post(url, json=body) as response:
                if response.status >= 400:
                    raise Exception(f"Failed to notify: {await response.text()}")


class MockNotifier(Notifier):
    def notify(self, message: str) -> None:
        log.info(f"MockNotifier.notify({message})")

    async def notify_async(self, message: str):
        log.info(f"MockNotifier.notify_async({message})")
