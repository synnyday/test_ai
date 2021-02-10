from sqlalchemy import create_engine, Column, Integer, String, text
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from settings import DB_PASSWORD, DB_HOST, DB_NAME, DB_USER
from utils import get_logger

logger = get_logger("db_connector")

CONNECTION_STRING = 'postgresql://{user}:{password}@{host}/{db_name}'.format(
    user=DB_USER, password=DB_PASSWORD, host=DB_HOST, db_name=DB_NAME)


engine = create_engine(CONNECTION_STRING, echo=False)
session = None
Base = declarative_base()


class ConsumerMessage(Base):
    __tablename__ = "consumer_message"

    id = Column(Integer, primary_key=True, autoincrement=True)
    key = Column(String)
    val = Column(String)
    topic = Column(String)
    partition = Column(Integer)
    offset = Column(Integer)

    def __init__(self, key, val, topic, partition, offset):
        self.key = key
        self.val = val
        self.topic = topic
        self.partition = partition
        self.offset = offset


# Create all the tables in the database
Base.metadata.create_all(engine)


def load_db_session():
    global session
    s_session = sessionmaker()
    s_session.configure(bind=engine)
    session = s_session()


def save_message(c_message):
    load_db_session()
    consumer_message = ConsumerMessage(**c_message)
    session.add(consumer_message)
    session.commit()
    return consumer_message.id
