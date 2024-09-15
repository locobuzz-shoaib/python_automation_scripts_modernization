import json
from datetime import datetime

import click

from clickhouse_connection import ClickhouseHandler
from modules.clickhouse_queries import query_clickhouse, query_mention_details
from modules.utils import format_mention_details, extract_values_from_final_data, generate_human_readable_summary


def parse_json_fields(data):
    """
    Parse any JSON string fields into Python dictionaries.
    Specifically handles 'latest_message' field to ensure it remains a dictionary.
    """
    json_fields = ['latest_message']  # Add any fields here that should be treated as JSON objects
    for field in json_fields:
        if field in data and isinstance(data[field], str):
            try:
                data[field] = json.loads(data[field])
            except (json.JSONDecodeError, TypeError):
                pass  # Leave the field as-is if it can't be parsed as JSON
    return data


@click.command()
@click.argument('social_ids', nargs=-1)  # Allow multiple social_ids
@click.option('--date_check', required=False,
              help="Date in 'YYYY-MM-DD' format. Defaults to current date if not provided.")
@click.option('--mode', required=True,
              help="Mode in which to run the script. Use 'oneline' to only return mention_id and summary.")
def run_query(social_ids, date_check, mode):
    """
    Runs queries for multiple social_ids on raw stream, final data, and dead letter tables,
    and checks if data is inserted in mentiondetails using the extracted categoryid, brandid, and channelgroupid.
    """
    # Default to current date if date_check is not provided
    if not date_check:
        date_check = datetime.utcnow().strftime('%Y-%m-%d')

    clickhouse_obj = ClickhouseHandler()

    for social_id in social_ids:
        social_id = social_id.strip()  # Strip any extra spaces or special characters from social_id
        try:
            # Query raw stream data
            raw_result = query_clickhouse(clickhouse_obj, 'testing.tbl_rawstream', social_id, date_check)

            # Query final data
            final_result = query_clickhouse(clickhouse_obj, 'testing.tbl_finaldata', social_id, date_check)

            # Query dead letter data
            deadletter_result = query_clickhouse(clickhouse_obj, 'testing.tbl_deadletter', social_id, date_check)

            # Generate human-readable summary
            human_readable_summary = generate_human_readable_summary({
                'raw_stream': raw_result,
                'final_data': final_result,
                'dead_letter': deadletter_result
            })

            # Extract categoryid, brandid, and channelgroupid from the final data
            categoryid, brandid, channelgroupid = extract_values_from_final_data(final_result)

            mention_id = None
            # If extracted values are valid, query the mentiondetails table
            if categoryid and brandid and channelgroupid:
                mention_details = query_mention_details(clickhouse_obj, categoryid, brandid, channelgroupid, social_id)
                if mention_details:
                    formatted_details = format_mention_details(mention_details)
                    mention_id = formatted_details  # Assuming the first part is the mention_id

            parse_config = {
                'json_parse': ['raw_result', 'final_result', 'deadletter_result'],
                'skip_fd_json_parse': ['raw_result'],
                'skip_rd_json_parse': ['final_result'],
                'skip_rf_json_parse': ['deadletter_result'],
                'skip_dl_json_parse': ['raw_result', 'final_result'],
                'skip_rm_json_parse': ['final_result', 'deadletter_result'],
                'skip_fi_json_parse': ['raw_result', 'deadletter_result']
            }

            # Get the parse fields for the current mode
            fields_to_parse = parse_config.get(mode, [])

            # Parse only the fields that match the mode
            if 'raw_result' in fields_to_parse:
                raw_result = parse_json_fields(raw_result)
            if 'final_result' in fields_to_parse:
                final_result = parse_json_fields(final_result)
            if 'deadletter_result' in fields_to_parse:
                deadletter_result = parse_json_fields(deadletter_result)

            # If 'oneline' mode is specified, return only the mention_id and summary
            if mode == 'oneline':
                result = {
                    'mention_id': mention_id,
                    'summary': human_readable_summary
                }
                click.echo(json.dumps(result, indent=2))
                continue  # Skip the rest of the processing for online mode

            # Combine all results, with summary as the first key
            combined_result = {
                'mention_id': mention_id,
                'summary': human_readable_summary,
                'raw_stream': raw_result if raw_result else {},
                'final_data': final_result if final_result else {},
                'dead_letter': deadletter_result if deadletter_result else {}
            }

            # Print the combined result with mention_id and summary
            click.echo(json.dumps(combined_result, indent=2))

        finally:
            # Ensure the ClickHouse connection is closed after each iteration
            clickhouse_obj.close()


if __name__ == "__main__":
    run_query()
