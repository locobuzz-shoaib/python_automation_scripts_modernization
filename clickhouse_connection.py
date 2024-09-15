import gc
import time
from typing import Union

import pandas as pd
from clickhouse_connect import dbapi
from pandas import DataFrame

from config import CLICKHOUSE_HOST, CLICKHOUSE_PORT, CLICKHOUSE_PASSWORD, CLICKHOUSE_USERNAME, G_CHAT, LOGGER, ENVIRON

if ENVIRON in ["PRODUCTION", "production"]:
    LOGGER.info("Connecting to production clickhouse")
    CLICKHOUSE_AUTH = {
        'host': CLICKHOUSE_HOST, 'port': CLICKHOUSE_PORT, 'secure': True, 'verify': True,
        'username': CLICKHOUSE_USERNAME, 'password': CLICKHOUSE_PASSWORD, "max_query_size": 20000000
    }
else:
    LOGGER.info("Connecting to staging clickhouse")
    CLICKHOUSE_AUTH = {'host': CLICKHOUSE_HOST, 'port': CLICKHOUSE_PORT, 'secure': False, 'verify': False,
                       "max_query_size": 20000000}


# max_insert_block_size
class ClickhouseHandler:
    def __init__(self):
        self.clickhouse = None
        self.clickhouse_cursor = None
        self.retry_sleep = 5

        try:
            # print(f"\033[1;33m[INFO][{self.__class__.__name__}] CXN OPEN\033[m")
            self.reinit_db()
        except Exception:
            text = f"First time Connection to ch Database via ClickhouseHandler failed, retrying " \
                   f"{2} times"

            for number in range(2):
                try:
                    self.reinit_db()
                    break
                except Exception:
                    self.clickhouse = None
                    self.clickhouse_cursor = None
                    time.sleep(self.retry_sleep)
                    pass
            if not self.clickhouse:
                text = f"In execute(), ch cursor initialisation failed, " \
                       f"after {2} retries"
                G_CHAT.send_message(text, 0)
                raise ConnectionError("Failed to connect to Clickhouse db")

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        gc.collect()

    def reinit_db(self):
        self.clickhouse = dbapi.Connection(**CLICKHOUSE_AUTH)
        self.clickhouse_cursor = self.clickhouse.cursor()

    def reinit_cursor(self):
        try:
            self.clickhouse_cursor = self.clickhouse.cursor()
        except Exception:
            self.reinit_db()

    @staticmethod
    def _console_logs(message: str, is_error: bool):
        if is_error:
            LOGGER.warning(f"\033[0;31m{message}\033[m")
        else:
            LOGGER.warning(message)

    def execute(self, query: str, values: Union[int, str, tuple] = ()):
        self.reinit_cursor()
        query = "\n".join([s.strip() for s in query.split("\n") if s.strip() and s != "\n"])
        # self._console_logs(f"QUERY CH:\n{query} ", False)
        try:
            if values:
                self.clickhouse_cursor.execute(query, values)
            else:
                self.clickhouse_cursor.execute(query)
        except Exception as e:
            self._console_logs(f"Query Execution failed on clickhouse, \n{query}, \nerror => {e}", True)
        gc.collect()

    def commit(self):
        self.clickhouse.commit()

    def fetch_df(self):
        headers = [tup[0] for tup in self.clickhouse_cursor.description]
        data = self.clickhouse_cursor.fetchall()
        if data or (not data and headers):
            df = DataFrame.from_records(data, columns=headers)
        else:
            df = pd.DataFrame()
        return df

    def close(self):
        self.clickhouse_cursor.close()
        self.clickhouse.close()
