"""
Not for production usage. Throw away scripts below. Reference use only.
"""

import csv
import functools
import os
from io import StringIO
from typing import Dict

from alive_progress import alive_bar
import arrow
import boto3
import click
import exchange_calendars as ec
import pandas as pd
import requests
from tenacity import retry, wait_fixed, stop_after_attempt
from mypy_boto3_s3.client import S3Client


@click.group()
def cli():
    pass


@cli.command()
@click.argument("start-date")
def save_eod_data(start_date):
    s3 = boto3.client("s3")
    # assuming same trading calendar for NYSE & NASDAQ
    nyse = ec.get_calendar("NYSE")
    # start_date = arrow.utcnow().shift(years=-30)
    start_date = arrow.get(start_date)
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
            bar()


@cli.command()
@click.argument("start-date")
@click.argument("end-date")
def update_market_cap(start_date, end_date):
    click.echo(f"{start_date} - {end_date}")
    s3_client = boto3.client("s3")
    s3_bucket = "prod-stock-universe"
    nyse = ec.get_calendar("NYSE")
    backfill_sessions = nyse.sessions_in_range(start_date.datetime, end_date.datetime)
    with alive_bar(len(backfill_sessions), title="Updating history with market cap") as bar:
        for session in backfill_sessions:
            session_date = arrow.get(session.date())
            bar.text(session_date.format("YYYY-MM-DD"))
            df = get_eod_prices(s3_client, s3_bucket, session_date)
            # filter out all rows except where Ticker == AAPL
            df = df[df["Ticker"] == "AAPL"]
            # for every ticker in df, get outstanding shares by calling the outstanding_shares function
            # then add to the df
            # below code is not tested (co-pilot suggestion)
            df["market_cap"] = df.apply(
                lambda row: get_market_cap(row["Ticker"], row["adjusted_close"], arrow.get(row["date"])), axis=1
            )
            # write the df to s3
            save_to_s3(s3_client, session_date, df.to_dict(orient="records"), "us")
            bar()


def get_market_cap(ticker: str, share_price: float, trading_day: arrow.arrow.Arrow) -> float:
    df = get_quarterly_outstanding_shares(ticker)
    # filter df to exclude all index datetimes in the future compared to date
    df = df[df.index <= pd.to_datetime(trading_day.date())].sort_index(ascending=False)
    outstanding_shares = df.iloc[0]["shares"]
    assert isinstance(outstanding_shares, float)
    return outstanding_shares * share_price


def get_eod_prices(s3_client: S3Client, s3_bucket: str, trading_day: arrow.arrow.Arrow) -> pd.DataFrame:
    """
    Gets the EOD price for all tickers on the given trading day.
    NOTE: Normally we would memoize this function calling out to s3 but since we
    are iterating through each date, we won't be duplicating calls. Caching would
    only serve to bloat memory footprint.
    """
    trading_day_formatted = trading_day.format("YYYY/MM/DD")
    s3_path = f"{trading_day_formatted}/prices/us.csv"
    s3_response = s3_client.get_object(Bucket=s3_bucket, Key=s3_path)
    df = pd.read_csv(s3_response["Body"], index_col=0)
    df["Ticker"] = df.index
    return df


@functools.cache
@retry(wait=wait_fixed(65), stop=stop_after_attempt(3))
def get_quarterly_outstanding_shares(ticker: str) -> pd.DataFrame:
    url = f"https://eodhistoricaldata.com/api/fundamentals/{ticker}.US"
    params = {"api_token": os.environ["EOD_DATA_API_KEY"], "filter": "outstandingShares::quarterly"}
    response = requests.get(url, params=params)
    response.raise_for_status()
    # load the json response as a pandas dataframe with dateFormatted as the index
    df = pd.read_json(response.text, orient="index", convert_dates=["date"])
    df = df.drop(columns=["dateFormatted"]).set_index("date")
    return df


def save_to_s3(s3, session_date, data, exchange):
    bucket = "prod-stock-universe"
    upload_key = f"{session_date.format('YYYY/MM/DD')}/prices/{exchange}.csv"
    stream = StringIO()
    headers = data[0].keys()
    writer = csv.DictWriter(stream, fieldnames=headers)
    writer.writeheader()
    writer.writerows(data)
    s3.put_object(Body=stream.getvalue().encode("utf-8"), Bucket=bucket, Key=upload_key)


@retry(wait=wait_fixed(65), stop=stop_after_attempt(3))
def get_prices(url: str, params: dict) -> requests.Response:
    response = requests.get(url, params=params)
    response.raise_for_status()
    return response


if __name__ == "__main__":
    cli()
