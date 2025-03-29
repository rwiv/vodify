import json
from queue import Queue

from vtask.common.amqp import AmqpHelper
from .stdl_message_manager import StdlMessageManager
from ..schema import StdlDoneMsg, STDL_DONE_QUEUE


class StdlMessageHelper:
    def __init__(self, amqp: AmqpHelper, manager: StdlMessageManager):
        self.__amqp = amqp
        self.__manager = manager

    def archive(self, out_path: str):
        messages = self.__manager.consume_all(clear=True)
        _write_file(messages, out_path)

    def publish_by_archive(self, src_path: str, n: int):
        with open(src_path, "r") as f:
            message_dicts = json.loads(f.read())
        if not isinstance(message_dicts, list):
            raise ValueError("Expected list data")

        queue: Queue[StdlDoneMsg] = Queue()
        for msg_dict in message_dicts:
            queue.put(StdlDoneMsg(**msg_dict))

        conn, chan = self.__amqp.connect()
        for _ in range(n):
            if queue.empty():
                break
            body = queue.get_nowait().model_dump_json(by_alias=True).encode("utf-8")
            self.__amqp.publish(chan, STDL_DONE_QUEUE, body)
        self.__amqp.close(conn)

        rest_messages = []
        while not queue.empty():
            rest_messages.append(queue.get_nowait())

        _write_file(rest_messages, src_path)


def _write_file(messages: list[StdlDoneMsg], out_path: str):
    with open(out_path, "w") as f:
        message_dicts = [msg.model_dump(mode="json", by_alias=True) for msg in messages]
        f.write(json.dumps(message_dicts, indent=2))
