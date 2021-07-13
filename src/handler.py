import logging
import os

import arrow
import boto3

import stock_universe


logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def run(event, context):
    s3_bucket = os.environ['S3_BUCKET']
    universe_date = arrow.get(event["time"])
    logger.info(f"Pulling most recent universe of stocks from nasdaq based on date for {universe_date}")
    s3 = boto3.client('s3')
    stock_universe.fetch_latest_universe_of_stocks(s3, universe_date, s3_bucket)
    # pushing new data to s3 will trigger a lambda to grab prices for all stocks based on current quote
    # make sure to filter out all ETFs based on `ETF==Y` flag
