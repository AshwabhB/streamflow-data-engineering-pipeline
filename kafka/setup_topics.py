import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError
from config.config import KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC


def create_topics():
    print(f"Connecting to Kafka at {KAFKA_BOOTSTRAP_SERVERS}...")
    admin = KafkaAdminClient(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        client_id="streamflow-admin",
    )

    topics_to_create = [
        NewTopic(name=KAFKA_TOPIC, num_partitions=3, replication_factor=1),
        NewTopic(name=f"{KAFKA_TOPIC}_dlq", num_partitions=1, replication_factor=1),
    ]

    try:
        admin.create_topics(new_topics=topics_to_create, validate_only=False)
        print(f"Created topics: {[t.name for t in topics_to_create]}")
    except TopicAlreadyExistsError:
        print("Topics already exist — skipping creation.")
    finally:
        admin.close()

    # List all topics to confirm
    admin2 = KafkaAdminClient(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS)
    existing = admin2.list_topics()
    admin2.close()
    print(f"All topics on broker: {sorted(existing)}")


if __name__ == "__main__":
    create_topics()
