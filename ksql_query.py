import requests
import json
import time
import sys
import asyncio
import httpx
import datetime


# ['1830493429751087104', 1, 0, 1, 0, 2]

async def main():
    ksql_url = "http://k8s-prodloco-ksqldbse-da5031855f-8c906df574fade88.elb.ap-south-1.amazonaws.com/query"
    ksql_query = {
        "ksql": """SELECT *
                          FROM FINAL_AGGREGATED_TABLE 
                          WHERE BRANDID = 12080 AND CATEGORYID = 1719
                    AND  STRINGTOTIMESTAMP(createddate, 'yyyy-MM-dd''T''HH:mm:ss')  >= 
                    STRINGTOTIMESTAMP('2024-09-11T05:03:45', 'yyyy-MM-dd''T''HH:mm:ss');""",
        "streamsProperties": {}
    }

    timeout = httpx.Timeout(connect=10.0, read=120.0, write=20.0, pool=120.0)
    try:
        async with httpx.AsyncClient(timeout=timeout) as client:
            async with client.stream(
                    "POST",
                    ksql_url,
                    headers={'Content-Type': 'application/vnd.ksql.v1+json'},
                    data=json.dumps(ksql_query)
            ) as response:

                # Check if the response status is not 200 OK
                if response.status_code != 200:
                    content = await response.aread()  # Reads entire response content
                    error_message = f"Received non-200 response: {response.status_code} - {content.decode('utf-8')}"
                    print(error_message)

                response.raise_for_status()  # Raise an error for other bad HTTP status codes

                # Process the streamed response in chunks
                data_dict = []  # List to store dictionaries with column names and values
                column_names = None
                utc_now = datetime.datetime.utcnow()
                formatted_utc_now = utc_now.strftime('%Y-%m-%dT%H:%M:%S')  # Current timestamp format

                partial_data = ""  # Store partial chunks for incomplete JSON objects
                last_received_time = time.time()
                timeout_period = 30  # Timeout after 30 seconds of inactivity

                try:
                    async for chunk in response.aiter_text():
                        partial_data += chunk.strip()

                        try:
                            decoded_line = json.loads(partial_data)
                            partial_data = ""  # Reset partial_data once the full JSON is processed

                            # Check if the timeout period has been exceeded
                            current_time = time.time()
                            if current_time - last_received_time > timeout_period:
                                print("No data received for the last 30 seconds, stopping the query.")
                                break

                            if isinstance(decoded_line, dict):
                                # This is the schema definition (sd) containing column names
                                if 'columnNames' in decoded_line:
                                    column_names = decoded_line['columnNames']
                                    print(f"Column names: {column_names}")

                            elif isinstance(decoded_line, list):
                                # This is the data (ss) containing values
                                if column_names:
                                    print("SS", decoded_line)
                                    last_received_time = time.time()  # Reset the timeout timer after receiving data


                        except json.JSONDecodeError:
                            # If JSON is incomplete, continue appending chunks until it forms a valid JSON
                            continue

                except httpx.ReadTimeout as e:
                    error_message = f"HTTP read timeout occurred: {e}"
                    print(error_message)


    except httpx.RequestError as e:
        error_message = f"HTTP request failed: {e}"
        print(error_message)


if __name__ == "__main__":
    asyncio.run(main())
