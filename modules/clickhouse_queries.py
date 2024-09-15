# modules/clickhouse_queries.py
from datetime import datetime

import click

from clickhouse_connection import ClickhouseHandler


def query_clickhouse(clickhouse_obj: ClickhouseHandler, table_name: str, social_id: str, date_check: str = "") -> dict:
    """
    General function to query ClickHouse tables.
    """
    try:
        if not date_check:
            date_check = datetime.utcnow().strftime('%Y-%m-%d 00:00:00')
        else:
            date_check = f"{date_check} 00:00:00"

        query = f"""
           SELECT JSONExtractUInt(message, 'Brand', 'BrandID'), *
           FROM {table_name}
           PREWHERE InsertedAt > toDateTime('{date_check}')
           WHERE message LIKE '%{social_id}%'
           """
        clickhouse_obj.execute(query)
        result_df = clickhouse_obj.fetch_df()

        result = {}
        if not result_df.empty:
            result['length'] = len(result_df)
            last_inserted_date = result_df['InsertedAt'].max()
            last_inserted_date_hr = last_inserted_date.strftime('%Y-%m-%d %H:%M:%S')
            latest_row = result_df.loc[result_df['InsertedAt'] == last_inserted_date]
            result['latest_message'] = latest_row['message'].values[0] if not latest_row.empty else None
            result['last_inserted_date'] = last_inserted_date_hr

        return result
    except Exception as e:
        click.echo(f"Error querying {table_name}: {str(e)}")
        return {}


def query_mention_details(clickhouse_obj: ClickhouseHandler, categoryid: int, brandid: int, channelgroupid: int,
                          social_id: str):
    """
    Query the mentiondetails table to check if the data is inserted.
    """
    try:
        query = f"""
        SELECT numlikescount, brandid, channeltype, tagid, instagramgraphapiid, isbrandpost,createddate
        FROM spatialrss.mentiondetails
        PREWHERE categoryid = {categoryid} AND brandid = {brandid}
        WHERE channelgroupid = {channelgroupid} AND tweetidorfbid = '{social_id}'
        """
        clickhouse_obj.execute(query)
        result_df = clickhouse_obj.fetch_df()

        if not result_df.empty:
            return result_df.to_dict(orient='records')
        else:
            return None

    except Exception as e:
        click.echo(f"Error querying mention details: {str(e)}")
        return None
