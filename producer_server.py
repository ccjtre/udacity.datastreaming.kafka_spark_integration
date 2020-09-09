import logging
logger = logging.getLogger(__name__)

from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka import Producer

import json
import time

class ProducerServer:
    """Defines and provides common functionality amongst Producers"""

    # Tracks existing topics across all Producer instances
    existing_topics = set([])

    def __init__(
        self,
        bootstrap_servers,
        client_id,
        topic_name,
        input_file,
        num_partitions=1,
        num_replicas=1,
    ):
        """Initializes a Producer object with basic settings"""
        self.bootstrap_servers = bootstrap_servers
        self.client_id = client_id
        self.topic_name = topic_name
        self.input_file = input_file
        self.num_partitions = num_partitions
        self.num_replicas = num_replicas

        self.broker_properties = {
            "bootstrap.servers": self.bootstrap_servers
        }
        
        self.adminclient = AdminClient({"bootstrap.servers": self.broker_properties.get("bootstrap.servers")})

        # If the topic does not already exist, try to create it
        if self.topic_name not in ProducerServer.existing_topics:
            self.create_topic()
            ProducerServer.existing_topics.add(self.topic_name)

        self.producer = Producer(self.broker_properties)

    def create_topic(self):
        """Creates the producer topic if it does not already exist"""
        logger.info(f"Creating topic: {self.topic_name}")
        futures = self.adminclient.create_topics(
            [
                NewTopic(
                    topic=self.topic_name,
                    num_partitions=self.num_partitions,
                    replication_factor=self.num_replicas,
                    config={
                        "cleanup.policy": "delete",
                        "compression.type": "lz4",
                        "delete.retention.ms": "2000",
                        "file.delete.delay.ms": "2000",
                    },
                )
            ]
        )
        
        for topic, future in futures.items():
            try:
                future.result()
            except Exception as e:
                if 36 == e.args[0].code():
                    logger.info(f"topic {topic} already exists")
                    pass
                else:
                    logger.error(f"failed to create topic {topic}: {e}")
                    raise
                    
    def generate_data(self):
        logger.info("Generating data")
        with open(self.input_file) as f:
            row_count = 0
            json_data = json.load(f)
            for row in json_data:
                row_count += 1
                logger.debug(f"row_count: {row_count}")
                message = self.dict_to_binary(row)
                self.producer.produce(topic=self.topic_name, value=message)
                time.sleep(0.1)
        logger.info("Data generation complete")
        return
        
    def dict_to_binary(self, json_dict):
        return json.dumps(json_dict).encode('utf-8')

    def close(self):
        """Prepares the producer for exit by cleaning up the producer"""
        logger.info("Shutting down producer, flushing queue...")
        self.producer.flush()
        return