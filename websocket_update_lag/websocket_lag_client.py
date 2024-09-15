import asyncio
import websockets
import json


async def display_lag_in_terminal(group_id, topic_name):
    uri = "ws://localhost:8765"
    async with websockets.connect(uri) as websocket:
        # Send group_id and topic_name to the server
        await websocket.send(json.dumps({'group_id': group_id, 'topic_name': topic_name}))

        while True:
            try:
                # Receive and display lag information from the server
                message = await websocket.recv()
                lag_info = json.loads(message)

                print(f"Lag details for consumer group '{group_id}' and topic '{topic_name}':")
                print(f"{'Partition':<10}{'Committed Offset':<20}{'Latest Offset':<20}{'Lag':<10}")
                print("=" * 60)

                for info in lag_info:
                    print(
                        f"{info['partition']:<10}{info['committed_offset']:<20}{info['latest_offset']:<20}{info['lag']:<10}")

                print("\nWaiting for the next update...\n")

            except websockets.ConnectionClosed:
                print("WebSocket connection closed")
                break
            except Exception as e:
                print(f"Error: {e}")
                break


if __name__ == "__main__":
    group_id = input("Enter the Kafka consumer group: ")
    topic_name = input("Enter the Kafka topic name: ")
    asyncio.run(display_lag_in_terminal(group_id, topic_name))
