from abc import ABC, abstractmethod


class Job(ABC):
    def __init__(self, name: str):
        self.name = name

    @abstractmethod
    async def run(self):
        pass
