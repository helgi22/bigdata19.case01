import asyncio
import csv
import sys

import aiofiles
from aiohttp import ClientSession
from tqdm import tqdm

import config as cfg

"""
Assignment 02
=============

The goal of this assignment is to implement synchronous scraping using standard python modules,
and compare the scraping speed to asynchronous mode.

Run this code with

    > fab run assignment02.py
"""

from yahoo import read_symbols, YAHOO_HTMLS

YAHOO_HTMLS = cfg.BUILDDIR / 'yahoo_html'

NASDAQ_FILES = (
    cfg.DATADIR / 'nasdaq' / 'amex.csv',
    cfg.DATADIR / 'nasdaq' / 'nasdaq.csv',
    cfg.DATADIR / 'nasdaq' / 'nyse.csv',
)


def read_symbols():
    """Read symbols from NASDAQ dataset"""
    symbols = set()
    for filename in NASDAQ_FILES:
        with open(filename) as f:
            reader = csv.DictReader(f)
            for row in reader:
                symbols.add(row['Symbol'].upper().strip())

    return list(sorted(symbols))


def scrape_descriptions_sync():
    """Scrape companies descriptions synchronously."""
    # TODO: Second assignment. Use https://docs.python.org/3/library/urllib.html
    symbols = read_symbols()
    progress = tqdm(total=len(symbols), file=sys.stdout, disable=False)
    YAHOO_HTMLS.mkdir(parents=True, exist_ok=True)

    headers = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_5) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/12.1.1 Safari/605.1.15',
    }

    def fetch(symbol, session):
        with session.request.urlopen(f'https://finance.yahoo.com/quote/{symbol}/profile?p={symbol}') as response:
            # with session.get(f'https://finance.yahoo.com/quote/{symbol}/profile?p={symbol}') as response:
            text = response.read()
            with aiofiles.open(YAHOO_HTMLS / f'{symbol}.html', 'wb') as f:
                f.write(text)
            progress.update(1)

    def run(symbols):
        with ClientSession(headers=headers) as session:
            tasks = (asyncio.ensure_future(fetch(symbol, session)) for symbol in symbols)
            asyncio.gather(*tasks)

    loop = asyncio.get_event_loop()
    # loop.set_exception_handler(lambda x, y: None)  # suppress exceptions because of bug in Python 3.7.3 + aiohttp + asyncio
    # loop.run_until_complete(asyncio.ensure_future(run(symbols)))
    progress.close()


def main():
    scrape_descriptions_sync()


if __name__ == '__main__':
    main()
