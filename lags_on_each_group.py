import argparse

import plotly.graph_objects as go
from confluent_kafka import TopicPartition, ConsumerGroupTopicPartitions, KafkaException, OFFSET_END
from confluent_kafka.admin import AdminClient, OffsetSpec
from jinja2 import Template

from config import BROKER

KAFKA_BOOTSTRAP_SERVERS = BROKER


def get_consumer_groups_for_topic(admin_client, topic_name):
    """
    Fetch all consumer groups consuming from a specific topic.
    Deduplicates consumer groups to avoid multiple appearances.
    """
    consumer_groups_result = admin_client.list_consumer_groups()

    topic_consumer_groups = set()  # Use a set to ensure unique entries
    for group in consumer_groups_result.result().valid:
        group_id = group.group_id
        try:
            group_description_result = admin_client.describe_consumer_groups([group_id])
            group_description = group_description_result[group_id].result()

            # Check if any members of the consumer group are consuming from the specified topic
            for member in group_description.members:
                for tp in member.assignment.topic_partitions:
                    if tp.topic == topic_name:
                        topic_consumer_groups.add(group_id)  # Add to set for uniqueness
                        break

        except KafkaException as e:
            print(f"Error describing group {group_id}: {e}")

    return list(topic_consumer_groups)


def create_plotly_chart(partition_lags, group_id, topic_name):
    """
    Create an interactive plotly bar chart for partition lags.
    """
    partitions = [p[0] for p in partition_lags]
    lags = [p[3] if isinstance(p[3], int) else 0 for p in partition_lags]

    fig = go.Figure(
        data=[
            go.Bar(x=partitions, y=lags, marker_color='lightblue')
        ]
    )
    fig.update_layout(
        title=f"Lag per Partition for Consumer Group '{group_id}' on Topic '{topic_name}'",
        xaxis_title="Partitions",
        yaxis_title="Lag",
        template="plotly_white"
    )
    return fig


def generate_html_report(partition_lags, group_id, topic_name, chart_html):
    """
    Generate an HTML report with partition lag details and an interactive chart.
    """
    html_template = """
    <html>
    <head>
        <title>Lag Report for {{ topic_name }}</title>
        <style>
            table {
                width: 100%;
                border-collapse: collapse;
                margin-top: 20px;
            }
            table, th, td {
                border: 1px solid black;
            }
            th, td {
                padding: 10px;
                text-align: center;
            }
            th {
                background-color: lightgray;
            }
        </style>
    </head>
    <body>
        <h1>Lag Report for Topic: {{ topic_name }}</h1>
        <h2>Consumer Group: {{ group_id }}</h2>

        <h3>Lag Details:</h3>
        <table>
            <tr>
                <th>Partition</th>
                <th>Committed Offset</th>
                <th>Latest Offset</th>
                <th>Lag</th>
            </tr>
            {% for partition, committed, latest, lag in partition_lags %}
            <tr>
                <td>{{ partition }}</td>
                <td>{{ committed }}</td>
                <td>{{ latest }}</td>
                <td>{{ lag }}</td>
            </tr>
            {% endfor %}
        </table>

        <h3>Lag Visualization:</h3>
        {{ chart_html|safe }}
    </body>
    </html>
    """

    template = Template(html_template)
    return template.render(partition_lags=partition_lags, group_id=group_id, topic_name=topic_name,
                           chart_html=chart_html)


def get_consumer_lag_for_group(admin_client, topic_name, group_id):
    """
    Get consumer lag for a specific consumer group and topic.
    """
    # Fetch partitions for the topic
    cluster_metadata = admin_client.list_topics(timeout=10)
    topic_metadata = cluster_metadata.topics.get(topic_name)

    if not topic_metadata:
        print(f"Topic '{topic_name}' not found.")
        return None, None

    # Fetch the committed offsets for the consumer group
    partitions = [TopicPartition(topic_name, p.id) for p in topic_metadata.partitions.values()]
    consumer_group_partitions = ConsumerGroupTopicPartitions(group_id, partitions)

    committed_offsets_result = admin_client.list_consumer_group_offsets([consumer_group_partitions])[group_id].result()

    # Fetch the latest offsets from Kafka
    latest_offsets = admin_client.list_offsets({p: OffsetSpec.latest() for p in partitions})


    # Collect lag data for sorting
    partition_lags = []

    for committed_offset in committed_offsets_result.topic_partitions:
        latest_offset = latest_offsets[committed_offset].result().offset
        lag = latest_offset - committed_offset.offset if latest_offset is not None else "N/A"
        partition_lags.append((committed_offset.partition, committed_offset.offset, latest_offset, lag))

    # Sort the partitions by lag in descending order
    partition_lags.sort(key=lambda x: x[3] if isinstance(x[3], int) else 0, reverse=True)

    # Return lag data
    return partition_lags


def get_consumer_lag_for_all_groups(admin_client, topic_name, generate_html):
    """
    Get consumer lag for all consumer groups associated with a topic.
    """
    # Get all consumer groups consuming from the topic
    consumer_groups = get_consumer_groups_for_topic(admin_client, topic_name)

    if consumer_groups:
        for group_id in consumer_groups:
            partition_lags = get_consumer_lag_for_group(admin_client, topic_name, group_id)
            if partition_lags:
                print(f"\nLag details for consumer group '{group_id}' and topic '{topic_name}':")
                print(f"{'Partition':<10}{'Committed Offset':<20}{'Latest Offset':<20}{'Lag':<10}")
                print("=" * 60)

                for partition, committed, latest, lag in partition_lags:
                    print(f"{partition:<10}{committed:<20}{latest:<20}{lag:<10}")

                # Create plotly chart
                fig = create_plotly_chart(partition_lags, group_id, topic_name)
                chart_html = fig.to_html(full_html=False)

                # Generate HTML report
                if generate_html:
                    html_report = generate_html_report(partition_lags, group_id, topic_name, chart_html)
                    with open(f"lag_report_{group_id}.html", "w", encoding="utf-8") as f:
                        f.write(html_report)
                        print(f"HTML report generated for consumer group '{group_id}'")
    else:
        print(f"No consumer groups are consuming from topic '{topic_name}'.")


if __name__ == "__main__":
    # Command-line argument parsing
    parser = argparse.ArgumentParser(description="Kafka Consumer Lag Checker with Graph and HTML Report")
    parser.add_argument('--topic', required=True, help="The Kafka topic name")
    parser.add_argument('--html', action='store_true', help="Generate HTML report")
    args = parser.parse_args()

    # Create the AdminClient
    admin_client = AdminClient({'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS})

    # Fetch and display the lag for all consumer groups associated with the topic
    get_consumer_lag_for_all_groups(admin_client, args.topic, args.html)
