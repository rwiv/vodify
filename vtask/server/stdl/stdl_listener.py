import json
import threading

from pika.adapters.blocking_connection import BlockingChannel
from pika.spec import Basic, BasicProperties
from pyutils import stacktrace_dict, log

from ...celery import stdl_done_local, stdl_done_remote, LOCAL_QUEUE_NAME, REMOTE_QUEUE_NAME
from ...common.amqp import Amqp
from ...common.fs import LOCAL_FILE_NAME
from ...service.stdl import StdlDoneMsg


class StdlListener:
    def __init__(self, amqp: Amqp, queue_name: str):
        self.amqp = amqp
        self.queue_name = queue_name

    def run(self):
        def run():
            conn, chan = self.amqp.connect()
            self.amqp.assert_queue(chan, self.queue_name)
            self.amqp.consume(chan, self.queue_name, self.on_message)
            self.amqp.close(conn)

        thread = threading.Thread(target=run)
        thread.start()
        return thread

    def on_message(self, ch: BlockingChannel, method: Basic.Deliver, props: BasicProperties, body: bytes):
        try:
            msg = StdlDoneMsg(**json.loads(body.decode("utf-8")))
            dct = msg.model_dump(mode="json", by_alias=True)
            if msg.fs_name == LOCAL_FILE_NAME:
                stdl_done_local.apply_async(args=[dct], queue=LOCAL_QUEUE_NAME)  # type: ignore
            else:
                stdl_done_remote.apply_async(args=[dct], queue=REMOTE_QUEUE_NAME)  # type: ignore
            ch.basic_ack(method.delivery_tag)
            log.info("stdl.done task sent", dct)
        except:
            log.error("stdl.done task failed", stacktrace_dict())
