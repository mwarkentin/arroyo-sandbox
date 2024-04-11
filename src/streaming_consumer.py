from typing import Mapping

from arroyo.backends.kafka import (
    KafkaConsumer,
    KafkaPayload,
    KafkaProducer,
)
from arroyo.backends.kafka.configuration import (
    build_kafka_consumer_configuration,
    build_kafka_configuration,
)
from arroyo.backends.kafka.consumer import KafkaConsumer
from arroyo.processing.strategies import Produce
from arroyo.commit import ONCE_PER_SECOND
from arroyo.processing import StreamProcessor
from arroyo.processing.strategies import (
    CommitOffsets,
    ProcessingStrategy,
    ProcessingStrategyFactory,
    Produce,
    RunTask,
)
from arroyo.types import Commit, Message, Partition, Topic


BOOTSTRAP_SERVERS = ["127.0.0.1:9092"]
TOPIC = Topic("source-topic")

def handle_message(message: Message[KafkaPayload]) -> Message[KafkaPayload]:
    print(f"handle_message: {message.payload}")
    return message

class ConsumerStrategyFactory(ProcessingStrategyFactory[KafkaPayload]):
    """
    The factory manages the lifecycle of the `ProcessingStrategy`.
    A strategy is created every time new partitions are assigned to the
    consumer, while it is destroyed when partitions are revoked or the
    consumer is closed
    """
    def create_with_partitions(
        self,
        commit: Commit,
        partitions: Mapping[Partition, int],
    ) -> ProcessingStrategy[KafkaPayload]:
        return RunTask(handle_message, CommitOffsets(commit))

consumer = KafkaConsumer(
    build_kafka_consumer_configuration(
        default_config={},
        bootstrap_servers=["127.0.0.1:9092"],
        auto_offset_reset="latest",
        group_id="test-group",
    )
)

consumer.subscribe([TOPIC])

processor = StreamProcessor(
    consumer=consumer,
    topic=TOPIC,
    processor_factory=ConsumerStrategyFactory(),
    commit_policy=ONCE_PER_SECOND,
)

processor.run()

# while True:
#     msg = consumer.poll(timeout=1.0)
#     if msg is not None:
#         print(f"Loop MSG: {msg.payload}")
