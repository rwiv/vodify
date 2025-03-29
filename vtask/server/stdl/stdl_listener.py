import json
import threading

from pika.adapters.blocking_connection import BlockingChannel, BlockingConnection
from pika.spec import Basic, BasicProperties
from pyutils import stacktrace_dict, log

from ...celery import stdl_done_local, stdl_done_remote, LOCAL_QUEUE_NAME, REMOTE_QUEUE_NAME
from ...common.amqp import AmqpHelper
from ...common.fs import LOCAL_FILE_NAME
from ...service.stdl.schema import StdlDoneMsg


class StdlListener:
    def __init__(self, amqp: AmqpHelper, queue_name: str):
        self.amqp = amqp
        self.queue_name = queue_name
        self.conn: BlockingConnection | None = None
        self.listen_thread: threading.Thread | None = None

    def start_consume(self):
        def run():
            conn, chan = self.amqp.connect()
            self.conn = conn
            self.amqp.ensure_queue(chan, self.queue_name)
            self.amqp.consume(chan, self.queue_name, self.on_message)
            self.amqp.close(conn)

        self.listen_thread = threading.Thread(target=run)
        self.listen_thread.start()

    def stop_consume(self):
        conn = self.conn
        if conn is not None:

            def close_conn():
                self.amqp.close(conn)
                self.conn = None

            conn.add_callback_threadsafe(close_conn)

        if self.listen_thread:
            self.listen_thread.join()
            self.listen_thread = None

    def on_message(self, ch: BlockingChannel, method: Basic.Deliver, props: BasicProperties, body: bytes):
        try:
            msg = StdlDoneMsg(**json.loads(body.decode("utf-8")))
            dct = msg.model_dump(mode="json", by_alias=True)
            if msg.fs_name == LOCAL_FILE_NAME:
                stdl_done_local.apply_async(args=[dct], queue=LOCAL_QUEUE_NAME)  # type: ignore
            else:
                stdl_done_remote.apply_async(args=[dct], queue=REMOTE_QUEUE_NAME)  # type: ignore
            ch.basic_ack(delivery_tag=method.delivery_tag)
            log.info("stdl.done task sent", dct)
        except:
            log.error("stdl.done task failed", stacktrace_dict())
