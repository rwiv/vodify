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

    def run_task(self):
        self.__push_all()
        while True:
            txt = self.__redis.rpop(self.__key)
            if txt is None:
                break
            if not isinstance(txt, bytes):
                raise ValueError("Expected bytes data")
            stdl_msg = StdlDoneMsg(**json.loads(txt.decode("utf-8")))
            print(stdl_msg)

    def consume_all(self):
        self.__push_all()
        return self.__get_all()

    def clear_list(self):
        self.__redis.delete(self.__key)

    def __get_all(self) -> list[StdlDoneMsg]:
        messages = self.__redis.lrange(self.__key, 0, -1)
        if not isinstance(messages, list):
            raise ValueError("Expected list data")
        return [StdlDoneMsg(**json.loads(msg.decode("utf-8"))) for msg in messages]

    def __push_all(self):
        conn, chan = self.__amqp.connect()
        try:
            while True:
                amqp_msg = self.__amqp.read_one(chan, STDL_DONE_QUEUE)
                if amqp_msg is None:
                    return
                stdl_msg = StdlDoneMsg(**json.loads(amqp_msg.body))
                stdl_msg_dict = stdl_msg.model_dump(mode="json", by_alias=True)
                self.__redis.lpush(self.__key, json.dumps(stdl_msg_dict))
                chan.basic_ack(delivery_tag=amqp_msg.method.delivery_tag)
        except Exception as e:
            log.error("stdl task failed", error_dict(e))
        finally:
            self.__amqp.close(conn)
