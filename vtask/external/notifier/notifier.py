from abc import ABC, abstractmethod

from pyutils import log


class Notifier(ABC):
    @abstractmethod
    async def notify(self, message: str):
        pass


class MockNotifier(Notifier):
    async def notify(self, message: str):
        log.info(f"MockNotifier.notify_async({message})")
