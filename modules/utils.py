# modules/utils.py

import json
from typing import Dict

import click
import pytz
from insta_webhook_utils_modernization.rules_objects_for_comments import datetime
from termcolor import colored
from tzlocal import get_localzone


def covert_utc_to_local(last_inserted_date_str: str) -> str:
    # Automatically detect the local timezone
    last_inserted_date = datetime.strptime(last_inserted_date_str, '%Y-%m-%d %H:%M:%S')
    local_timezone = get_localzone()

    # Convert the UTC datetime to the local timezone
    utc_timezone = pytz.utc
    utc_time = utc_timezone.localize(last_inserted_date)  # Ensure the datetime is aware of its timezone
    localized_time = utc_time.astimezone(local_timezone)
    return localized_time.strftime('%Y-%m-%d %H:%M:%S %Z')


def generate_human_readable_summary(combined_result: Dict) -> str:
    """
    Generate a human-readable summary of the results, including logic to compare final and dead letter data.
    """
    summary = []

    # Raw stream summary
    if 'raw_stream' in combined_result and combined_result['raw_stream']:
        raw = combined_result['raw_stream']
        summary.append(
            f"{raw['length']} entries in raw model with the latest entry at {covert_utc_to_local(raw['last_inserted_date'])}")
    else:
        summary.append("No entries in the raw stream model.")

    # Final data summary
    if 'final_data' in combined_result and combined_result['final_data']:
        final = combined_result['final_data']
        summary.append(
            f"{final['length']} entries in final data model with the latest entry at {covert_utc_to_local(final['last_inserted_date'])}")
    else:
        summary.append("No entries in the final data model.")

    # Dead letter summary
    if 'dead_letter' in combined_result and combined_result['dead_letter']:
        dead_letter = combined_result['dead_letter']
        summary.append(
            f"{dead_letter['length']} entries in dead letter model with the latest entry at {covert_utc_to_local(dead_letter['last_inserted_date'])}")
    else:
        summary.append("No entries in the dead letter model.")

    # Check if final data needs to be inserted into ClickHouse
    if 'final_data' in combined_result and 'dead_letter' in combined_result:
        final = combined_result.get('final_data', {})
        dead_letter = combined_result.get('dead_letter', {})

        if final and dead_letter:
            final_date = final.get('last_inserted_date', '')
            dead_letter_date = dead_letter.get('last_inserted_date', '')
            if final_date and dead_letter_date and final_date > dead_letter_date:
                summary.append("Data needs to be inserted in ClickHouse.")

        # If final data is empty, but raw and dead letter have entries
        elif not final and 'raw_stream' in combined_result and combined_result['raw_stream'] and dead_letter:
            red_message = colored("Data is inserted into dead letter.", "red")
            summary.append(red_message)

    return " | ".join(summary)


def format_mention_details(mention_details: list) -> str:
    """
    Format the mention details in the format brandid/channeltype/tagid with brand or user activity.
    """
    formatted_details = []
    for detail in mention_details:
        channel_type = detail.get('channeltype', 'N/A')
        brandid = detail.get('brandid', 'N/A')
        tag_id = detail.get('tagid', 'N/A')
        is_brand_post = detail.get('isbrandpost', 0)
        created_time = detail.get('createddate', 'N/A')
        # Determine if it's brand activity or user activity
        activity_type = "brand activity" if is_brand_post else "user activity"
        if created_time != 'N/A':
            activity_type += f" at {covert_utc_to_local(str(created_time))}  and utc time is {str(created_time)}"
        formatted_details.append(f"{brandid}/{channel_type}/{tag_id} {activity_type}")

    return " | ".join(formatted_details)


def extract_values_from_final_data(final_data: dict):
    """
    Extracts categoryid, brandid, and channelgroupid from the final data message.
    """
    try:
        message = json.loads(final_data.get('latest_message', '{}'))
        brandid = message.get('Brand', {}).get('BrandID')
        categoryid = message.get('Brand', {}).get('CategoryID')
        channelgroupid = message.get('Mention', {}).get('ChannelGroupID')
        return categoryid, brandid, channelgroupid
    except Exception as e:
        click.echo(f"Error extracting values from final data: {str(e)}")
        return None, None, None
