import logging
from confluent_kafka.admin import AdminClient, NewTopic
from colorama import Fore, Style

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Your Kafka configuration
kafka_config = {
    "bootstrap_servers": "77.81.230.104:9092",
    "username": "admin",
    "password": "VawEzo1ikLtrA8Ug8THa",
    "security_protocol": "SASL_PLAINTEXT",
    "sasl_mechanism": "PLAIN",
}

# Initialize Kafka Admin client
admin_client = AdminClient({
    "bootstrap.servers": kafka_config["bootstrap_servers"],
    "security.protocol": kafka_config["security_protocol"],
    "sasl.mechanisms": kafka_config["sasl_mechanism"],
    "sasl.username": kafka_config["username"],
    "sasl.password": kafka_config["password"],
})

my_name = "alina_ryabova"
athlete_event_results = f"{my_name}_athlete_event_results"
enriched_athlete_avg = f"{my_name}_enriched_athlete_avg"
num_partitions = 2
replication_factor = 1

# Define topics
topics = [
    NewTopic(athlete_event_results, num_partitions, replication_factor),
    NewTopic(enriched_athlete_avg, num_partitions, replication_factor),
]

# Deleting topics (if they exist)
try:
    delete_futures = admin_client.delete_topics(
        [athlete_event_results, enriched_athlete_avg]
    )
    for topic, future in delete_futures.items():
        try:
            future.result()
            logger.info(f"Topic '{topic}' deleted successfully.")
        except Exception as e:
            logger.error(f"Failed to delete topic '{topic}': {e}")
except Exception as e:
    logger.error(f"An error occurred during topic deletion: {e}")

# Creating topics
try:
    create_futures = admin_client.create_topics(topics)
    for topic, future in create_futures.items():
        try:
            future.result()
            logger.info(f"Topic '{topic}' created successfully.")
        except Exception as e:
            logger.error(f"Failed to create topic '{topic}': {e}")
except Exception as e:
    logger.error(f"An error occurred during topic creation: {e}")

# Listing existing topics
try:
    metadata = admin_client.list_topics(timeout=10)
    for topic in metadata.topics.keys():
        if my_name in topic:
            logger.warning(f"Topic '{topic}' already exists.")
except Exception as e:
    logger.error(f"Failed to list topics: {e}")
