import argparse
import asyncio
import logging
import os
import sys

from crawler import Crawler
from utils import init_logging


async def main():
    parser = argparse.ArgumentParser(prog="douyin crawler")
    parser.add_argument(
        "--phone", type=str, required=False, help="phone number to login as"
    )
    subparsers = parser.add_subparsers(required=True)
    search_parser = subparsers.add_parser("search", description="search keywords")
    search_parser.add_argument("KEYWORDS", type=list)

    detail_parser = subparsers.add_parser(
        "detail", description="show details about awemes"
    )
    detail_parser.add_argument("AWEMES", type=list)

    args = parser.parse_args()

    logger = init_logging(
        logging.getLevelName(os.getenv("LOGGING_LEVEL") or "INFO"), persistent=False
    )

    logger.info(args)

    cw = Crawler(awemes=["7306880126984211724"], logger=logger)
    await cw.start()


if __name__ == "__main__":
    try:
        asyncio.get_event_loop().run_until_complete(main())
    except KeyboardInterrupt:
        sys.exit()
