import argparse
import asyncio
import json

import websockets
from confluent_kafka import TopicPartition, ConsumerGroupTopicPartitions
from confluent_kafka.admin import AdminClient, KafkaException, OffsetSpec


KAFKA_BOOTSTRAP_SERVERS = "b-2.locobuzzuatmskcluster.4psd4m.c3.kafka.ap-south-1.amazonaws.com:9092,b-1.locobuzzuatmskcluster.4psd4m.c3.kafka.ap-south-1.amazonaws.com:9092"

# WebSocket configuration
WEBSOCKET_HOST = 'localhost'
WEBSOCKET_PORT = 8765

admin_client = AdminClient({'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS})


async def fetch_consumer_lag(group_id, topic_name):
    """
    Fetch the consumer lag for a given consumer group and topic.
    """
    try:
        # Fetch partitions for the topic
        cluster_metadata = admin_client.list_topics(timeout=10)
        topic_metadata = cluster_metadata.topics.get(topic_name)

        if not topic_metadata:
            return None

        # Fetch the committed offsets for the consumer group
        partitions = [TopicPartition(topic_name, p.id) for p in topic_metadata.partitions.values()]
        consumer_group_partitions = [ConsumerGroupTopicPartitions(group_id, partitions)]
        committed_offsets_result = admin_client.list_consumer_group_offsets(consumer_group_partitions).result()
        committed_offsets = {tp.partition: committed_offsets_result[tp].offset for tp in committed_offsets_result.topic_partitions}

        # Fetch the latest offsets from Kafka
        latest_offsets_result = admin_client.list_offsets({p: OffsetSpec.latest() for p in partitions}).result()
        latest_offsets = {tp.partition: latest_offsets_result[tp].offset for tp in latest_offsets_result}

        # Prepare lag information
        lag_info = [
            {
                'partition': p.partition,
                'committed_offset': committed_offsets.get(p.partition, 'N/A'),
                'latest_offset': latest_offsets.get(p.partition, 'N/A'),
                'lag': latest_offsets[p.partition] - committed_offsets[
                    p.partition] if p.partition in committed_offsets and p.partition in latest_offsets else 'N/A'
            }
            for p in partitions
        ]

        return lag_info

    except KafkaException as e:
        print(f"Error fetching consumer lag: {e}")
        return None


async def send_lag_updates(websocket, path, group_id, topic_name):
    """
    Sends Kafka consumer group lag information over WebSocket in real-time.
    """
    try:
        while True:
            lag_info = await fetch_consumer_lag(group_id, topic_name)

            if lag_info:
                # Send lag data to the WebSocket client
                await websocket.send(json.dumps(lag_info, indent=2))

            # Sleep for 5 seconds before fetching the next update
            await asyncio.sleep(5)

    except websockets.ConnectionClosed as e:
        print(f"WebSocket connection closed: {e}")

    except Exception as e:
        print(f"Error: {e}")

    finally:
        print("Cleaning up after WebSocket connection")
        await websocket.close()


async def websocket_handler(websocket, path):
    """
    WebSocket handler that handles incoming connections and sends updates.
    """
    try:
        # Get group_id and topic_name from client-side request
        async for message in websocket:
            data = json.loads(message)
            group_id = data['group_id']
            topic_name = data['topic_name']

            # Call the function to send lag updates
            await send_lag_updates(websocket, path, group_id, topic_name)

    except Exception as e:
        print(f"WebSocket error: {e}")


# Start the WebSocket server
async def start_websocket_server():
    async with websockets.serve(websocket_handler, WEBSOCKET_HOST, WEBSOCKET_PORT):
        print(f"WebSocket server started on ws://{WEBSOCKET_HOST}:{WEBSOCKET_PORT}")
        await asyncio.Future()  # Keep the server running indefinitely


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Kafka WebSocket Server for Consumer Lag")
    parser.add_argument('--topic', required=True, help="Kafka topic name")
    parser.add_argument('--group_id', required=True, help="Kafka consumer group ID")
    args = parser.parse_args()

    asyncio.run(start_websocket_server())
