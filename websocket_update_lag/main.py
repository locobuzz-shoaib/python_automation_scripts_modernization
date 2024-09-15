import argparse
import os
import subprocess
import webbrowser


def run_websocket_server(topic, group_id):
    """
    Run the WebSocket server in a subprocess.
    """
    subprocess.Popen(['python', 'websocket_lag_server.py', '--topic', topic, '--group_id', group_id])


def run_html_server():
    """
    Serve the HTML report using a simple HTTP server.
    """
    os.chdir('templates')  # Go to the directory where HTML is stored
    subprocess.Popen(['python', '-m', 'http.server', '8080'])

    # Automatically open the browser to view the HTML
    webbrowser.open("http://localhost:8080/lag_report.html")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Kafka Consumer Lag WebSocket + HTML Report")
    parser.add_argument('--topic', required=True, help="Kafka topic name")
    parser.add_argument('--group_id', required=True, help="Kafka consumer group ID")
    args = parser.parse_args()

    # Run WebSocket server and HTML server
    run_websocket_server(args.topic, args.group_id)
    run_html_server()
