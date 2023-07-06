
import sys
import logging
from logging import config

import pandas as pd

from confs.log_config import LOG_CONFIG
from data_processors.stock_screener import StockScreener
from confs.config import FILTER_EXCHANGE_LIST
from confs.config import FILTER_MIN_PRICE
from confs.config import FILTER_MAX_PRICE
from confs.config import FILTER_AVG_VOLUME
from confs.config import FILTER_KEEP_ETF
from confs.config import FILTER_KEEP_FUND

def setup_logging():
    config.dictConfig(LOG_CONFIG)


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    setup_logging()
    pd.set_option('display.max_columns', None)
    logging.info("test")
    s = StockScreener(filter_exchange_list=FILTER_EXCHANGE_LIST, filter_min_price=FILTER_MIN_PRICE,
                      filter_max_price=FILTER_MAX_PRICE, filter_avg_volume=FILTER_AVG_VOLUME,
                      filter_keep_etf=FILTER_KEEP_ETF, filter_keep_fund=FILTER_KEEP_FUND)
    s.select_stocks()

# See PyCharm help at https://www.jetbrains.com/help/pycharm/
