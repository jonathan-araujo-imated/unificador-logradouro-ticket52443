import logging
import sys

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt='%d/%m/%Y %H:%M:%S',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)


def info(m):
    logging.info(m)


def error(m):
    logging.error(m)


def warning(m):
    logging.warning(m)
