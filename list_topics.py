from confluent_kafka.admin import AdminClient, KafkaException
from prettytable import PrettyTable  # You can use prettytable to format the output
from config import BROKER

# Kafka Configuration
KAFKA_BOOTSTRAP_SERVERS = BROKER


def list_kafka_topics_and_consumer_groups():
    """
    List all Kafka topics and associated consumer groups in a formatted table.
    """
    # Create an admin client to interact with the Kafka cluster
    admin_client = AdminClient({
        'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS
    })

    # Fetch metadata for all topics in the Kafka cluster
    cluster_metadata = admin_client.list_topics(timeout=10)

    # Fetch the list of consumer groups
    consumer_groups_result = admin_client.list_consumer_groups()

    try:
        # Get the valid consumer groups from the result
        consumer_groups = consumer_groups_result.result()  # Resolve future

        # Fetch detailed information for each consumer group
        topic_consumer_map = {}

        # Iterate through each consumer group and describe its details
        for group in consumer_groups.valid:
            group_id = group.group_id
            try:
                group_description_result = admin_client.describe_consumer_groups([group_id])
                group_description = group_description_result[group_id].result()  # Access result by group_id

                # Ensure the group description contains members
                if group_description.members:
                    # For each member, get the topics and partitions they're consuming
                    for member in group_description.members:
                        # Access the topic_partitions from the assignment
                        for tp in member.assignment.topic_partitions:
                            topic = tp.topic  # Get the topic name from the TopicPartition object
                            if topic not in topic_consumer_map:
                                topic_consumer_map[topic] = set()
                            topic_consumer_map[topic].add(group_id)

            except KafkaException as e:
                print(f"Error describing group {group_id}: {e}")

        # Prepare table for printing
        table = PrettyTable()
        table.field_names = ["Topic", "Consumer Groups"]

        for topic in cluster_metadata.topics:
            groups = topic_consumer_map.get(topic, [])
            groups_list = ", ".join(groups) if groups else "No Consumer Groups"
            table.add_row([topic, groups_list])

        # Print the formatted table
        print(table)

    except Exception as e:
        print(f"Error fetching consumer groups: {e}")


if __name__ == "__main__":
    list_kafka_topics_and_consumer_groups()
