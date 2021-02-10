from kafka import KafkaConsumer
from json import loads, dumps
from settings import BOOTSTRAP_SERVER, CONSUMER_TIMEOUT_MS
from settings import TOPIC, TIMEOUT_MS
from utils import get_logger
from db_connector import save_message

logger = get_logger("consumer")


def run_consumer():
    cfg = {
        'bootstrap_servers': [BOOTSTRAP_SERVER],
        'consumer_timeout_ms': CONSUMER_TIMEOUT_MS,
        'value_deserializer': lambda x: loads(x.decode('utf-8'))
    }

    consumer = KafkaConsumer(TOPIC, **cfg)

    while True:
        msg_pack = consumer.poll(timeout_ms=TIMEOUT_MS)

        for tp, messages in msg_pack.items():
            for message in messages:
                dat = {
                    'key': message.key,
                    'val': dumps(message.value).encode('utf-8'),
                    'partition': tp.partition,
                    'topic': tp.topic,
                    'offset': message.offset
                }
                m_id = save_message(dat)
                logger.info("Saved with id {} message ---> {}:{:d}:{:d}: key={} value={}".
                            format(m_id, tp.topic, tp.partition, message.offset,
                                   message.key, message.value))
