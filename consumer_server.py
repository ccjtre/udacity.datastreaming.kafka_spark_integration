"""Defines core consumer functionality"""
import logging
import logging.config

logging.config.fileConfig('logging.ini')
logger = logging.getLogger(__name__)

import confluent_kafka
from confluent_kafka import Consumer

import tornado.ioloop
from tornado import gen

class ConsumerServer:
    """Defines the base kafka consumer class"""

    def __init__(
        self,
        topic_name_pattern,
        offset_earliest=False,
        sleep_secs=1.0,
        consume_timeout=0.1,
    ):
        """Creates a consumer object for asynchronous use"""
        self.topic_name_pattern = topic_name_pattern
        self.sleep_secs = sleep_secs
        self.consume_timeout = consume_timeout
        self.offset_earliest = offset_earliest

        self.broker_properties = {
            "bootstrap.servers": "PLAINTEXT://localhost:9093",
            "group.id": "GRP.0",
            "max.poll.interval.ms": 600000
        }
        
        self.consumer = Consumer(
            {
                "bootstrap.servers": self.broker_properties.get("bootstrap.servers"),
                "group.id": self.broker_properties.get("group.id"),
                "max.poll.interval.ms": self.broker_properties.get("max.poll.interval.ms")
            }
        )

        self.consumer.subscribe([self.topic_name_pattern], on_assign=self.on_assign)

    def on_assign(self, consumer, partitions):
        """Callback for when topic assignment takes place"""
 
        if self.offset_earliest is True:
            for partition in partitions:
                partition.offset = confluent_kafka.OFFSET_BEGINNING

        logger.info("partitions assigned for %s", self.topic_name_pattern)
        self.consumer.assign(partitions)

    async def consume(self):
        """Asynchronously consumes data from kafka topic"""
        while True:
            num_results = 1
            while num_results > 0:
                num_results = self._consume()
            await gen.sleep(self.sleep_secs)

    def _consume(self):
        """Polls for a message. Returns 1 if a message was received, 0 otherwise"""

        message = self.consumer.poll(self.consume_timeout)
        if message is None:
            logger.debug("No message received on topic %s", self.topic_name_pattern)
            return 0
        elif message.error() is not None:
            logger.error(f"Error in receiving message from topic {self.topic_name_pattern}: {message.error()}")
            return 1
        else:
            logger.debug(f"Received message from topic {self.topic_name_pattern}:\n {message.value()}")
            return 1

    def close(self):
        """Cleans up any open kafka consumers"""
        self.consumer.unsubscribe()
        
def read_feed():
    consumerserver = ConsumerServer(
        topic_name_pattern="SFPD.EVENTS.CALLS_FOR_SERVICE"
    )
    
    try:
        tornado.ioloop.IOLoop.current().spawn_callback(consumerserver.consume)
        tornado.ioloop.IOLoop.current().start()
    except KeyboardInterrupt as e:
        tornado.ioloop.IOLoop.current().stop()
        consumerserver.close()
        exit(0)

if __name__ == "__main__":
    read_feed()