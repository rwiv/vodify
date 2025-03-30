import json

from pyutils import log, error_dict
from redis import Redis

from ..schema import StdlDoneMsg, STDL_DONE_QUEUE
from ....common.amqp import AmqpHelper
from ....common.env import RedisConfig


REDIS_STDL_DONE_LIST_KEY = "vtask:stdl:done"


class StdlMessageManager:
    def __init__(self, amqp: AmqpHelper, conf: RedisConfig):
        self.__amqp = amqp
        self.__redis = Redis(host=conf.host, port=conf.port, password=conf.password, db=0)
        self.__key = REDIS_STDL_DONE_LIST_KEY

    def consume_all(self, clear: bool = True):
        self.push_all()
        messages = self.get_all()
        messages.reverse()
        if clear:
            self.clear_list()
        return messages

    def clear_list(self):
        self.__redis.delete(self.__key)

    def get_all(self) -> list[StdlDoneMsg]:
        messages = self.__redis.lrange(self.__key, 0, -1)
        if not isinstance(messages, list):
            raise ValueError("Expected list data")
        return [StdlDoneMsg(**json.loads(msg.decode("utf-8"))) for msg in messages]

    def pop(self) -> StdlDoneMsg | None:
        txt = self.__redis.rpop(self.__key)
        if txt is None:
            return None
        if not isinstance(txt, bytes):
            raise ValueError("Expected bytes data")
        return StdlDoneMsg(**json.loads(txt.decode("utf-8")))

    def publish(self, msg: StdlDoneMsg):
        conn, chan = self.__amqp.connect()
        self.__amqp.ensure_queue(chan, STDL_DONE_QUEUE, auto_delete=False)
        self.__amqp.publish(chan, STDL_DONE_QUEUE, msg.model_dump_json(by_alias=True).encode("utf-8"))
        self.__amqp.close(conn)

    def push_all(self):
        conn, chan = self.__amqp.connect()
        try:
            while True:
                amqp_msg = self.__amqp.read_one(chan, STDL_DONE_QUEUE)
                if amqp_msg is None:
                    return
                stdl_msg = StdlDoneMsg(**json.loads(amqp_msg.body))
                stdl_msg_json = stdl_msg.model_dump_json(by_alias=True)
                self.__redis.lpush(self.__key, stdl_msg_json)
                chan.basic_ack(delivery_tag=amqp_msg.method.delivery_tag)
        except Exception as e:
            log.error("stdl task failed", error_dict(e))
        finally:
            self.__amqp.close(conn)
