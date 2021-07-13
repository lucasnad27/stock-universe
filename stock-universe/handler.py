from datetime import datetime
import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def run(event, context):
    dt_string = event["time"]
    dt_format = "%Y-%m-%dT%H:%M:%SZ"
    universe_date = datetime.strptime(dt_string, dt_format)
    logger.info("Pulling most recent universe of stocks from nasdaq based on date for given datetime")
    logger.info(f"{universe_date=}")
    # calculate date and call function to pull data from nasdaq and push into s3
    pass
