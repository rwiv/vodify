from pyutils import log, error_dict

from ...common.amqp import AmqpHelper
from ...utils import RedisQueue


class AmqpRedisMigrator:
    def __init__(self, amqp: AmqpHelper, queue: RedisQueue, amqp_queue_name: str):
        self.__amqp = amqp
        self.__amqp_queue_name = amqp_queue_name
        self.__queue = queue

    def push_all_amqp_messages(self):
        conn, chan = self.__amqp.connect()
        try:
            while True:
                amqp_msg = self.__amqp.read_one(chan, self.__amqp_queue_name)
                if amqp_msg is None:
                    return
                self.__queue.push(amqp_msg.body)
                chan.basic_ack(delivery_tag=amqp_msg.method.delivery_tag)
        except Exception as e:
            log.error("stdl task failed", error_dict(e))
        finally:
            self.__amqp.close(conn)
