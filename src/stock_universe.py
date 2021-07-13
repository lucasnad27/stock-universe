import logging
import urllib.request as request
from contextlib import closing

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def fetch_latest_universe_of_stocks(s3, processing_date, bucket):
    filename = "nasdaqlisted.txt"
    url_to_open = f'ftp://ftp.nasdaqtrader.com/symboldirectory/{filename}'
    # get yesterday's date and format as YYYY/MM/DD
    s3_key = f"{processing_date.format('YYYY/MM/DD')}/universe"

    logger.info(f"Pulling nasdaq tickers, {url_to_open=}")
    with closing(request.urlopen(url_to_open)) as r:
        logger.info(f"Uploading file to s3: {s3_key=}")
        s3.upload_fileobj(r, bucket, f"{s3_key}/{filename}")
