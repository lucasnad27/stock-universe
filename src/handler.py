import asyncio
import logging
import os

import arrow
import boto3

import stock_universe


logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def get_stock_universe(event, context):
    s3_bucket = os.environ['S3_BUCKET']
    universe_date = arrow.get(event["time"])
    logger.info(f"Pulling most recent universe of stocks from nasdaq based on date for {universe_date}")
    s3 = boto3.client('s3')
    stock_universe.fetch_latest_universe_of_stocks(s3, universe_date, s3_bucket)


def process_eod_quotes(event, context):
    logger.info(event)
    assert len(event['Records']) == 1

    s3 = boto3.client('s3')
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = event['Records'][0]['s3']['object']['key']
    logger.info(f"Processing eod stock data from s3 bucket {bucket} with key {key}")

    loop = asyncio.get_event_loop()
    loop.run_until_complete(stock_universe.process_eod_quotes(s3, bucket, key))
    logger.info("*********Completed quote processing for universe*********")


def process_eod_fundamentals(event, context):
    s3 = boto3.client('s3')
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = event['Records'][0]['s3']['object']['key']
    logger.info(f"Processing fundamental stock data from s3 bucket {bucket} with key {key}")
    loop = asyncio.get_event_loop()
    loop.run_until_complete(stock_universe.process_eod_fundies(s3, bucket, key))
    logger.info("*********Completed quote processing for universe*********")
