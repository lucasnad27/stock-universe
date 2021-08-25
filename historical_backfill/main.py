"""
Not for production usage. Throw away scripts below. Reference use only.
"""

import csv
import functools
import os
from io import StringIO
from typing import Dict, Optional

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
    us_url = "https://eodhistoricaldata.com/api/eod-bulk-last-day/US"
    params = {"api_token": os.environ["EOD_DATA_API_KEY"], "fmt": "json", "filter": "extended"}
    with alive_bar(len(backfill_sessions), title="30 year backtest data pull") as bar:
        for session in backfill_sessions:
            session_date = arrow.get(session.date())
            bar.text(f"Pulling EOD pricing from {session_date.format('YYYY-MM-DD')}")
            params.update({"date": session_date.format("YYYY-MM-DD")})
            us_response = get_prices(us_url, params)
            save_to_s3(s3, session_date, us_response.json(), "us", "prices")
            bar()


@cli.command()
@click.argument("start-date")
@click.argument("end-date")
def update_market_cap(start_date, end_date):
    start_date = arrow.get(start_date)
    end_date = arrow.get(end_date)
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
            # for every ticker in df, get outstanding shares by calling the get_outstanding_shares function
            df["outstanding_shares"] = df.apply(
                lambda row: get_outstanding_shares(s3_client, s3_bucket, row["Ticker"], arrow.get(row["date"])), axis=1
            )
            df["market_cap"] = df["outstanding_shares"] * df["adjusted_close"]
            # drop MarketCapitalization from df
            df = df.drop(columns=["MarketCapitalization"])
            # write the df to s3
            save_to_s3(s3_client, session_date, df.to_dict(orient="records"), "us", "prices")
            bar()


def get_outstanding_shares(
    s3_client: S3Client, s3_bucket: str, ticker: str, trading_day: arrow.arrow.Arrow
) -> Optional[float]:
    click.echo(f"Getting outstanding shares for {ticker}")
    df = get_quarterly_outstanding_shares(s3_client, s3_bucket, ticker)
    if df.empty:
        return None
    # filter df to exclude all index datetimes in the future compared to date
    filtered_df = df[df.index <= pd.to_datetime(trading_day.date())].sort_index(ascending=False)
    if filtered_df.empty:
        # no outstanding share data exists for this date for this ticker, grab last known value
        filtered_df = df.sort_index()
    outstanding_shares = float(filtered_df.iloc[0]["shares"])
    return outstanding_shares


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
@retry(wait=wait_fixed(65), stop=stop_after_attempt(2))
def get_quarterly_outstanding_shares(s3_client: S3Client, s3_bucket: str, ticker: str) -> pd.DataFrame:
    upload_key = f"tickers/outstanding_shares_quarterly/{ticker}-US.csv"
    url = f"https://eodhistoricaldata.com/api/fundamentals/{ticker}.US"
    params = {"api_token": os.environ["EOD_DATA_API_KEY"], "filter": "outstandingShares::quarterly"}

    try:
        s3_response = s3_client.get_object(Bucket=s3_bucket, Key=upload_key)
        click.echo(f"Using s3 for quarterly share information for {ticker}")
        df = pd.read_csv(s3_response["Body"], index_col=0)
        return df
    except s3_client.exceptions.NoSuchKey:
        # share information has not been uploaded to s3 yet, call the API instead
        pass
    click.echo(f"Calling API for quarterly share information for {ticker}")
    response = requests.get(url, params=params)
    # This is gross, too many ways of telling me no data, argh
    if response.status_code == 404:
        # noop
        pass
    else:
        response.raise_for_status()
    invalid_responses = ["{}", '"NA"']
    if response.status_code == 404 or response.text in invalid_responses:
        # create an empty dataframe with columns shares_outstanding and date
        df = pd.DataFrame(columns=["shares_outstanding", "date", "dateFormatted"])
    else:
        df = pd.read_json(response.text, orient="index", convert_dates=["date"])
    df = df.drop(columns=["dateFormatted"]).set_index("date")
    stream = StringIO()
    df.to_csv(stream)
    # convert df to csv and upload to s3 based on the date
    s3_client.put_object(Body=stream.getvalue().encode("utf-8"), Bucket=s3_bucket, Key=upload_key)
    return df


def save_to_s3(
    s3: S3Client, session_date: Optional[arrow.arrow.Arrow], data: dict, file_name: str, data_type: str = "prices"
) -> None:
    bucket = "prod-stock-universe"
    if session_date:
        upload_key = f"{session_date.format('YYYY/MM/DD')}/{data_type}/{file_name}.csv"
    else:
        upload_key = f"{data_type}/{file_name}.csv"
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
