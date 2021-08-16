"""
Not for production usage. Throw away scripts below. Reference use only.
"""

import csv
import os
from io import StringIO

from alive_progress import alive_bar
import arrow
import boto3
import exchange_calendars as ec
import requests
from tenacity import retry, wait_fixed


def save_to_s3(s3, session_date, data, exchange):
    bucket = "prod-stock-universe"
    upload_key = f"{session_date.format('YYYY/MM/DD')}/prices/{exchange}.csv"
    stream = StringIO()
    headers = data[0].keys()
    writer = csv.DictWriter(stream, fieldnames=headers)
    writer.writeheader()
    writer.writerows(data)
    s3.put_object(Body=stream.getvalue().encode("utf-8"), Bucket=bucket, Key=upload_key)


@retry(wait=wait_fixed(86400))
def get_prices(url, params):
    response = requests.get(url, params=params)
    response.raise_for_status()
    return response


def main():
    s3 = boto3.client("s3")
    # assuming same trading calendar for NYSE & NASDAQ
    nyse = ec.get_calendar("NYSE")
    # start_date = arrow.utcnow().shift(years=-30)
    start_date = arrow.get("1992-07-13")
    end_date = arrow.utcnow()
    backfill_sessions = nyse.sessions_in_range(start_date.datetime, end_date.datetime)

    # EOD price, splits, and dividends for last 30 years
    # nasdaq_url = "https://eodhistoricaldata.com/api/eod-bulk-last-day/NASDAQ"
    # nyse_url = "https://eodhistoricaldata.com/api/eod-bulk-last-day/NYSE"
    us_url = "https://eodhistoricaldata.com/api/eod-bulk-last-day/US"
    params = {"api_token": os.environ["EOD_DATA_API_KEY"], "fmt": "json", "filter": "extended"}
    with alive_bar(len(backfill_sessions), title="30 year backtest data pull") as bar:
        for session in backfill_sessions:
            session_date = arrow.get(session.date())
            bar.text(f"Pulling EOD pricing from {session_date.format('YYYY-MM-DD')}")
            # params.update({"date": arrow.get(session.date()).format("YYYY-MM-DD"), "type": "dividends"})
            params.update({"date": session_date.format("YYYY-MM-DD")})
            us_response = get_prices(us_url, params)
            save_to_s3(s3, session_date, us_response.json(), "us")
            # nasdaq_response = get_prices(nasdaq_url, params)
            # save_to_s3(s3, session_date, nasdaq_response.json(), "xnas")
            # nyse_response = get_prices(nyse_url, params)
            # save_to_s3(s3, session_date, nyse_response.json(), "xnys")
            bar()


if __name__ == "__main__":
    main()

# sample EOD split response comes as a list
# {'code': 'WKSP',
#    'exchange': 'US',
#    'date': '2021-08-04',
#    'split': '1.000000/20.000000'
# }
# sample EOD dividend response comes as a list as well
# {'code': 'WINC',
#   'exchange': 'US',
#   'date': '2021-08-02',
#   'dividend': '0.03154'}
# sample EOD price response comes as a list (not actually json, sigh)
# we'll need to convert this to the much more comprehensive tdameritrade response for daily quotes
# {
#   'code': 'ZIXI',
#   'exchange_short_name': 'US',
#   'date': '1991-08-19',
#   'open': 15.875,
#   'high': 16.25,
#   'low': 15.251,
#   'close': 15.875,
#   'adjusted_close': 8.297,
#   'volume': 57000
# }
# column_lookup = {
#     "code": "Symbol",
#     "open": "openPrice",
#     "high": "highPrice",
#     "low": "lowPrice",
#     "close": "closePrice",
#     "adjusted_close": "adjClose",
#     "volume": "totalVolume",
# }
