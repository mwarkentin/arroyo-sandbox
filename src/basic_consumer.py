from arroyo.backends.kafka.configuration import (
    build_kafka_consumer_configuration,
)
from arroyo.backends.kafka.consumer import KafkaConsumer
from arroyo.types import Topic

TOPIC = Topic("source-topic")

consumer = KafkaConsumer(
    build_kafka_consumer_configuration(
        default_config={},
        bootstrap_servers=["127.0.0.1:9092"],
        auto_offset_reset="latest",
        group_id="test-group",
    )
)

consumer.subscribe([TOPIC])

while True:
    msg = consumer.poll(timeout=1.0)
    if msg is not None:
        print(f"MSG: {msg.payload}")
