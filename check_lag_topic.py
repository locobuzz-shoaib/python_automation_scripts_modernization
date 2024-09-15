from confluent_kafka import OFFSET_END, TopicPartition, ConsumerGroupTopicPartitions, KafkaException
from confluent_kafka.admin import AdminClient, OffsetSpec

from config import BROKER

KAFKA_BOOTSTRAP_SERVERS = BROKER


def get_consumer_groups_for_topic(admin_client, topic_name):
    """
    Fetch all consumer groups consuming from a specific topic.
    """
    consumer_groups_result = admin_client.list_consumer_groups()

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

    return topic_consumer_groups


def get_consumer_lag(topic_name, group_id):
    """
    Get consumer lag for a specific consumer group and topic.
    """
    admin_client = AdminClient({'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS})

    # Fetch partitions for the topic
    cluster_metadata = admin_client.list_topics(timeout=10)
    topic_metadata = cluster_metadata.topics.get(topic_name)

    if not topic_metadata:
        print(f"Topic '{topic_name}' not found.")
        return

    # Prepare TopicPartitions for the topic
    partitions = [TopicPartition(topic_name, p.id) for p in topic_metadata.partitions.values()]

    # Prepare the request for consumer group offsets
    consumer_group_partitions = [ConsumerGroupTopicPartitions(group_id, partitions)]

    # Fetch committed offsets for the consumer group
    committed_offsets_future = admin_client.list_consumer_group_offsets(consumer_group_partitions)
    committed_offsets_result = committed_offsets_future[group_id].result()  # Resolve the future for the group_id

    # Fetch the latest offsets from Kafka using OffsetSpec.latest()
    latest_offsets = admin_client.list_offsets({p: OffsetSpec.latest() for p in partitions})

    # You need to resolve each future in the latest_offsets dictionary
    resolved_latest_offsets = {p: future.result() for p, future in latest_offsets.items()}

    # Print the lag for each partition
    print(f"\nLag details for topic '{topic_name}' and consumer group '{group_id}':")
    print(f"{'Partition':<10}{'Committed Offset':<20}{'Latest Offset':<20}{'Lag':<10}")
    print("=" * 60)

    for tp in committed_offsets_result.topic_partitions:
        partition = tp.partition
        committed_offset = tp.offset
        latest_offset = resolved_latest_offsets.get(TopicPartition(topic_name, partition)).offset

        if committed_offset is not None and latest_offset is not None:
            lag = latest_offset - committed_offset
            print(f"{partition:<10}{committed_offset:<20}{latest_offset:<20}{lag:<10}")
        else:
            print(f"{partition:<10}{'N/A':<20}{'N/A':<20}{'N/A':<10}")


if __name__ == "__main__":
    # Create the AdminClient
    admin_client = AdminClient({'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS})

    # Get the topic name from the user
    topic = input("Enter the Kafka topic name: ")

    # Fetch consumer groups associated with the topic
    consumer_groups = get_consumer_groups_for_topic(admin_client, topic)

    if consumer_groups:
        print(f"Consumer groups consuming from topic '{topic}':")
        for i, group in enumerate(consumer_groups, 1):
            print(f"{i}. {group}")

        # Let the user select the consumer group
        selected_index = int(input("Select the consumer group by number (or type 'q' to quit): ")) - 1
        selected_group = consumer_groups[selected_index]

        # Fetch and display the lag for the selected consumer group
        get_consumer_lag(topic, selected_group)
    else:
        print(f"No consumer groups are consuming from topic '{topic}'.")

