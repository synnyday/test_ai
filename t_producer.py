from kafka import KafkaProducer
from time import sleep
from json import dumps
from settings import BOOTSTRAP_SERVER, PRODUCER_TIMEOUT_S
from settings import TOPIC, COMPRESSION_TYPE
from utils import get_logger

logger = get_logger("producer")


def run_producer():
    producer = KafkaProducer(bootstrap_servers=[BOOTSTRAP_SERVER],
                             value_serializer=lambda x: dumps(x).encode('utf-8'),
                             compression_type=COMPRESSION_TYPE)
    try:
        for e in range(1000):
            data = {'number': e}
            future = producer.send(TOPIC, value=data)
            record_metadata = future.get(timeout=PRODUCER_TIMEOUT_S)

            logger.info('The message has been sent to a topic: '
                        '{}, partition: {}, offset: {}'
                        .format(record_metadata.topic, record_metadata.partition,
                                record_metadata.offset))
            sleep(5)
    except Exception:
        logger.exception('Producer error occurred:')

    finally:
        producer.flush()
