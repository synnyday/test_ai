import click
from kafka import KafkaAdminClient
from kafka.admin import NewTopic
from settings import TOPIC, BOOTSTRAP_SERVER
from utils import get_logger
from t_producer import run_producer
from t_consumer import run_consumer

logger = get_logger("main")


@click.command()
@click.option('--producer', prompt='Start producer process', is_flag=True)
@click.option('--consumer', prompt='Start consumer process', is_flag=True)
@click.option('--topic_name', prompt='Create Kafka topic', is_flag=True)
def main(producer, consumer, topic_name):
    if topic_name:
        try:
            admin = KafkaAdminClient(bootstrap_servers=BOOTSTRAP_SERVER)

            topic = NewTopic(name=TOPIC,
                             num_partitions=1,
                             replication_factor=1)
            admin.create_topics([topic])
        except Exception:
            logger.exception('Error to create Kafka topic:')
    if producer:
        run_producer()
    if consumer:
        run_consumer()


if __name__ == "__main__":
    main()
