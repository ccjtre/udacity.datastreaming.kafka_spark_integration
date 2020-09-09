import logging
import logging.config

logging.config.fileConfig('logging.ini')
logger = logging.getLogger(__name__)

import producer_server

def feed():
    producerserver = producer_server.ProducerServer(
        bootstrap_servers="PLAINTEXT://localhost:9093",
        client_id="CLI.0",
        topic_name="SFPD.EVENTS.CALLS_FOR_SERVICE",
        input_file="/home/workspace/police-department-calls-for-service.json"
    )
    
    try:
        producerserver.generate_data()
    except KeyboardInterrupt as e:
        producerserver.close()
        exit(0)

if __name__ == "__main__":
    feed()
