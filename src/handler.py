import asyncio
import logging
import os

import arrow
import boto3

import stock_universe


logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def get_stock_universe(event, context):
    s3_bucket = os.environ["S3_BUCKET"]
    universe_date = arrow.get(event["time"])
    logger.info(f"Pulling most recent universe of stocks from nasdaq based on date for {universe_date}")
    now = arrow.utcnow()
    # Depending on the current date to know if we have a valid trading day is not ideal. If our job is ever delayed,
    # fails to run due to downtime, or if we change the cron schedule without understanding the consequences, we may
    # end up skipping processing on a valid trading day. We could also end up processing days when the exchanges are
    # not open. As we expand to other exchanges, we will need to address this, potentially coming up with a specific
    # schedule per exchange, depending on the data & apis available to us.
    if not stock_universe.is_valid_universe_date(now):
        return
    s3 = boto3.client("s3")
    stock_universe.fetch_latest_universe_of_stocks(s3, universe_date, s3_bucket)


def process_eod_quotes(event, context):
    logger.info(event)
    assert len(event["Records"]) == 1

    s3 = boto3.client("s3")
    bucket = event["Records"][0]["s3"]["bucket"]["name"]
    key = event["Records"][0]["s3"]["object"]["key"]
    logger.info(f"Processing eod stock data from s3 bucket {bucket} with key {key}")

    loop = asyncio.get_event_loop()
    loop.run_until_complete(stock_universe.process_eod_quotes(s3, bucket, key))
    logger.info("*********Completed quote processing for universe*********")


def process_eod_fundamentals(event, context):
    s3 = boto3.client("s3")
    bucket = event["Records"][0]["s3"]["bucket"]["name"]
    key = event["Records"][0]["s3"]["object"]["key"]
    logger.info(f"Processing fundamental stock data from s3 bucket {bucket} with key {key}")
    loop = asyncio.get_event_loop()
    loop.run_until_complete(stock_universe.process_eod_fundies(s3, bucket, key))
    logger.info("*********Completed quote processing for universe*********")
