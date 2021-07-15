import asyncio
import logging
import math
import os
import urllib.request as request
from contextlib import closing
from io import StringIO
from typing import List

import arrow
import numpy as np
import pandas as pd
from arrow.arrow import Arrow
from mypy_boto3_s3.client import S3Client
from tenacity import retry, wait

from client import TdClient


logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

CHUNK_SIZE = int(os.environ["CHUNK_SIZE"])
TD_MAX_CONCURRENT_REQUESTS = int(os.environ["TD_MAX_CONCURRENT_REQUESTS"])


def fetch_latest_universe_of_stocks(s3: S3Client, processing_date: Arrow, bucket: str) -> None:
    # fetch nasdaq
    nasdaq_filename = "nasdaqlisted.txt"
    base_url = "ftp://ftp.nasdaqtrader.com/symboldirectory/"
    fundie_s3_key = "incoming/fundamentals"
    quote_s3_key = "incoming/quotes"
    nasdaq_s3_filename = f"{processing_date.format('YYYY-MM-DD')}-{nasdaq_filename}"

    logger.info("Pulling nasdaq tickers")
    # TODO: pull data from ftp server into memory to reduce network traffic
    with closing(request.urlopen(f"{base_url}/{nasdaq_filename}")) as r:
        logger.info(f"Uploading {nasdaq_s3_filename} to s3: {fundie_s3_key=}")
        s3.upload_fileobj(Fileobj=r, Bucket=bucket, Key=f"{fundie_s3_key}/{nasdaq_s3_filename}")
    with closing(request.urlopen(f"{base_url}/{nasdaq_filename}")) as r:
        logger.info(f"Uploading {nasdaq_s3_filename} to s3: {quote_s3_key=}")
        s3.upload_fileobj(Fileobj=r, Bucket=bucket, Key=f"{quote_s3_key}/{nasdaq_s3_filename}")

    # fetch nyse
    logger.info("Pulling all other tickers")
    nyse_filename = "otherlisted.txt"
    nyse_s3_filename = f"{processing_date.format('YYYY-MM-DD')}-{nyse_filename}"
    with closing(request.urlopen(f"{base_url}/{nyse_filename}")) as r:
        logger.info(f"Uploading {nyse_s3_filename} to s3: {fundie_s3_key=}")
        s3.upload_fileobj(Fileobj=r, Bucket=bucket, Key=f"{fundie_s3_key}/{nyse_s3_filename}")
    with closing(request.urlopen(f"{base_url}/{nyse_filename}")) as r:
        logger.info(f"Uploading {nyse_s3_filename} to s3: {quote_s3_key=}")
        s3.upload_fileobj(Fileobj=r, Bucket=bucket, Key=f"{quote_s3_key}/{nyse_s3_filename}")


async def process_eod_quotes(s3: S3Client, bucket: str, key: str) -> None:
    file_obj = s3.get_object(Bucket=bucket, Key=key)
    # read file from s3 into pandas dataframe
    df = pd.read_csv(StringIO(file_obj["Body"].read().decode("utf-8")), delimiter="|")

    if "otherlisted" in key:
        df = _filter_nyse_file(df)
        exchange = "xnys"
        # this represents the name of the column that contains the ticker
        symbol_column = "ACT Symbol"
    elif "nasdaqlisted" in key:
        df = _filter_nasdaq_file(df)
        exchange = "xnas"
        # this represents the name of the column that contains the ticker
        symbol_column = "Symbol"
    else:
        raise RuntimeError("Unsupported file type, cannot process")

    ticker_chunks = _split_tickers(df, symbol_column)
    # this parallelizes multiple requests to the TD API, using asyncio + semaphore
    quotes = await get_quotes(ticker_chunks)

    # converts dictionary into pandas dataframe with the key being the index and label the index 'Symbol'
    quotes_df = pd.DataFrame.from_dict(quotes, orient="index")
    quotes_df.index.name = "Symbol"

    upload_key = f"{arrow.utcnow().format('YYYY/MM/DD')}/quotes/{exchange}.csv"
    stream = StringIO()
    quotes_df.to_csv(stream)
    # convert df to csv and upload to s3 based on the date
    s3.put_object(Body=stream.getvalue().encode("utf-8"), Bucket=bucket, Key=upload_key)
    logger.info(f"Successfully uploaded quote data: {upload_key}")


async def process_eod_fundies(s3: S3Client, bucket: str, key: str) -> None:
    file_obj = s3.get_object(Bucket=bucket, Key=key)
    df = pd.read_csv(StringIO(file_obj["Body"].read().decode("utf-8")), delimiter="|")

    if "otherlisted" in key:
        df = _filter_nyse_file(df)
        exchange = "xnys"
        # this represents the name of the column that contains the ticker
        symbol_column = "ACT Symbol"
    elif "nasdaqlisted" in key:
        df = _filter_nasdaq_file(df)
        exchange = "xnas"
        symbol_column = "Symbol"
    else:
        raise RuntimeError("Unsupported file type, cannot process")

    ticker_chunks = _split_tickers(df, symbol_column)
    fundies = await get_fundies(ticker_chunks)
    fundies_df = pd.DataFrame.from_dict(fundies, orient="index")
    fundies_df.index.name = "Symbol"

    upload_key = f"{arrow.utcnow().format('YYYY/MM/DD')}/fundamentals/{exchange}.csv"
    stream = StringIO()
    fundies_df.to_csv(stream)
    # convert df to csv and upload to s3 based on the date
    s3.put_object(Body=stream.getvalue().encode("utf-8"), Bucket=bucket, Key=upload_key)
    logger.info(f"Successfully uploaded fundamental data: {upload_key}")


def _filter_nasdaq_file(df: pd.DataFrame) -> pd.DataFrame:
    """To see schema of files, visit http://www.nasdaqtrader.com/trader.aspx?id=symboldirdefs"""
    # only keep records with Test Issue == 'N'
    df = df[df["Test Issue"] == "N"]
    # remove ETFs
    df = df[df["ETF"] == "N"]
    return df


def _filter_nyse_file(df: pd.DataFrame) -> pd.DataFrame:
    """To see schema of files, visit http://www.nasdaqtrader.com/trader.aspx?id=symboldirdefs"""
    file_creation_string = df.tail(1)["ACT Symbol"]
    logger.info(f"Doing nothing with {file_creation_string=}")
    # only keep records with Test Issue == 'N'
    df = df[df["Test Issue"] == "N"]
    # remove ETFs
    df = df[df["ETF"] == "N"]
    #  filter out to only show NYSE
    nyse_df = df[df["Exchange"] == "N"]
    return nyse_df


def _split_tickers(df: pd.DataFrame, symbol_column: str) -> List[pd.Series]:
    """Split tickers into chunks of CHUNK_SIZE"""
    num_chunks = math.ceil(len(df) / CHUNK_SIZE)
    ticker_chunks = np.array_split(df[symbol_column], num_chunks)
    return ticker_chunks


async def get_fundies(ticker_chunks: List[pd.Series]) -> dict:
    """Get fundamental data for a list of tickers"""
    client = TdClient()
    semaphore = asyncio.Semaphore(TD_MAX_CONCURRENT_REQUESTS)
    requests = []
    async with semaphore:
        requests = [
            asyncio.ensure_future(_get_fundies(semaphore, client, ticker_chunk.to_list()))
            for ticker_chunk in ticker_chunks
        ]
    responses = await asyncio.gather(*requests)
    fundies = {}
    for response in responses:
        fundies.update(response)
    return fundies


async def get_quotes(ticker_chunks: List[pd.Series]) -> dict:
    client = TdClient()
    semaphore = asyncio.Semaphore(TD_MAX_CONCURRENT_REQUESTS)
    requests = []
    async with semaphore:
        requests = [
            asyncio.ensure_future(_get_quotes(semaphore, client, ticker_chunk.to_list()))
            for ticker_chunk in ticker_chunks
        ]
    responses = await asyncio.gather(*requests)
    quotes = {}
    for response in responses:
        quotes.update(response)
    return quotes


@retry(wait=wait.wait_random_exponential(multiplier=1, max=10))
async def _get_quotes(semaphore: asyncio.Semaphore, client: TdClient, tickers: List[str]) -> dict:
    async with semaphore:
        response = client.get_quotes(tickers)
        response.raise_for_status()
        logger.info(f"Successfully grabbed quotes for {len(tickers)} tickers")
        return response.json()


@retry(wait=wait.wait_random_exponential(multiplier=1, max=10))
async def _get_fundies(semaphore: asyncio.Semaphore, client: TdClient, tickers: List[str]) -> dict:
    async with semaphore:
        response = client.search_instruments(tickers, client.Instrument.Projection.FUNDAMENTAL)
        response.raise_for_status()
        logger.info(f"Successfully grabbed fundamentals for {len(tickers)} tickers")
        return response.json()
