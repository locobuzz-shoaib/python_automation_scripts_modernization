from confluent_kafka.admin import AdminClient, KafkaException
from prettytable import PrettyTable  # For formatting the output
from config import BROKER

# Kafka Configuration
KAFKA_BOOTSTRAP_SERVERS = BROKER


def print_topic_details_and_consumer_groups(topic_name):
    """
    Print detailed information for a single Kafka topic and its associated consumer groups.
    """
    # Create an admin client to interact with the Kafka cluster
    admin_client = AdminClient({
        'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS
    })

    try:
        # Fetch metadata for the specific topic
        cluster_metadata = admin_client.list_topics(timeout=10)
        topic_metadata = cluster_metadata.topics.get(topic_name)

        if not topic_metadata:
            print(f"Topic '{topic_name}' not found.")
            return

        # Prepare table to display partition details
        table = PrettyTable()
        table.field_names = ["Partition", "Leader", "Replicas", "In-Sync Replicas", "Error"]

        # Iterate through partitions and print their details
        for partition in topic_metadata.partitions.values():
            partition_id = partition.id
            leader = partition.leader
            replicas = ", ".join(str(r) for r in partition.replicas)
            in_sync_replicas = ", ".join(str(isr) for isr in partition.isrs)
            error = partition.error if partition.error else "None"

            table.add_row([partition_id, leader, replicas, in_sync_replicas, error])

        # Print the detailed topic view in a table
        print(f"Details for Topic: {topic_name}")
        print(table)

        # Fetch consumer groups associated with this topic
        print(f"\nFetching consumer groups consuming from topic '{topic_name}'...\n")
        consumer_groups_result = admin_client.list_consumer_groups()  # No timeout argument

        # Fetch detailed information for each consumer group
        topic_consumer_groups = []

        for group in consumer_groups_result.result().valid:
            group_id = group.group_id
            try:
                group_description_result = admin_client.describe_consumer_groups([group_id])
                group_description = group_description_result[group_id].result()

                # Check if any members of the consumer group are consuming from the specified topic
                for member in group_description.members:
                    for tp in member.assignment.topic_partitions:
                        if tp.topic == topic_name:
                            topic_consumer_groups.append(group_id)
                            break

            except KafkaException as e:
                print(f"Error describing group {group_id}: {e}")

        # Display associated consumer groups
        if topic_consumer_groups:
            print(f"Consumer Groups consuming from topic '{topic_name}':")
            for group in topic_consumer_groups:
                print(f" - {group}")
        else:
            print(f"No consumer groups found for topic '{topic_name}'.")

    except Exception as e:
        print(f"Error fetching topic details or consumer groups: {e}")


if __name__ == "__main__":
    topic = input("Enter the Kafka topic name: ")
    print_topic_details_and_consumer_groups(topic)
