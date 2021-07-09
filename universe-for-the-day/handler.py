import datetime
import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def run(event, context):
    current_time = datetime.datetime.now().time()
    name = context.function_name
    logger.info("pre-flight", extra={"context": context, "event": event})
    logger.info("running", extra={"time": event["time"], "name": context.name})
