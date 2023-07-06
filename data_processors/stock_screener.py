"""Contains methods and classes to select stocks from
FMP API
"""
import concurrent.futures
import functools
from datetime import datetime, date, timedelta
from typing import Dict, List

import fmpsdk as fmp
import numpy as np
import pandas as pd
import logging
from confs.config import FMP_API_KEY
from confs.config import FILTER_EXCHANGE_LIST
from confs.config import FILTER_MIN_PRICE
from confs.config import FILTER_MAX_PRICE
from confs.config import FILTER_AVG_VOLUME
from confs.config import FILTER_KEEP_ETF
from confs.config import FILTER_COUNTRY_EXCLUSION
from confs.config import FILTER_KEEP_FUND
from db.database import Database


class StockScreener:
    """Provides methods to select interesting stock data from
    FMP API

    Attributes
    ----------
        start_date : str
            start date of the data (modified from neofinrl_config.py)
        end_date : str
            end date of the data (modified from neofinrl_config.py)
        ticker_list : list
            a list of stock tickers (modified from neofinrl_config.py)

    Methods
    -------
    select_stocks()
        Fetches data from FMP API

    """

    def __init__(self, filter_exchange_list:list[str], filter_min_price:int, filter_max_price:int, filter_avg_volume:int, filter_keep_etf:bool, filter_keep_fund:bool ):

        self.logger = logging.getLogger(__name__)

        self.filter_exchange_list = filter_exchange_list
        self.filter_min_price = filter_min_price
        self.filter_max_price = filter_max_price
        self.filter_avg_volume = filter_avg_volume
        self.logger = logging.getLogger(__name__)
        self.filter_keep_etf = filter_keep_etf
        self.filter_keep_fund = filter_keep_fund


    def __filter_exchange(self, stock: Dict):
        """
        Filter stock based on exchange criteria
        :param stock: stock to analyse and filter
        :return: True if stock exchange is in the filter criteria
        """
        symbol = stock['symbol']

        if (symbol[-3:] in self.filter_exchange_list):
            return True
        return False

    def __filter_type(self, stock: Dict):
        """
        Filter stock based on type (stock, etf)
        :param stock: stock to analyse and filter
        :return: True if stock type is in the filter criteria
        """
        type = stock['type']

        if (not self.filter_keep_etf):
            return type=='stock'
        return True

    def __filter_price(self, stock: Dict):
        """
        Filter stock based on current price
        :param stock: stock to analyse and filter
        :return: True if stock price is in the filter criteria
        """
        price = stock['price']

        if (price >= self.filter_min_price and price<=self.filter_max_price):
            return True
        return False



    # select stocks
    def select_stocks(self) -> pd.DataFrame:
        self.logger.info("Starting stock screening.")
        self.db = Database()
        # use fmpsdk in order to get all available stocks
        # ToDo : user fmp screener when optimized for such operations
        selected_stocks = fmp.available_traded_list(apikey=FMP_API_KEY)
        selected_stocks = filter(self.__filter_type, selected_stocks)
        selected_stocks = filter(self.__filter_exchange, selected_stocks)
        selected_stocks = filter(self.__filter_price, selected_stocks)
        l = list(selected_stocks)
        df = pd.DataFrame()  # Empty DataFrame
        i = len(l)
        for i in range(len(l)):
            try :
                tic = l[i]['symbol']
                cpd = fmp.company_profile(FMP_API_KEY, tic)
                result = cpd[0]
                self.logger.info("Processing stock : " + result['symbol'])
                ipo_date = datetime.strptime(
                    result['ipoDate'],
                    '%Y-%m-%d').date()
                ipo_date_lt_10_years = False
                if (ipo_date):
                    today = date.today()

                    delta = today - ipo_date
                    if (delta.days < 3650):
                        ipo_date_lt_10_years = True
                is_actively_trading = result['isActivelyTrading']
                vol_avg = result['volAvg']

                country = result['country']
                today = date.today()

                one_year_ago = today - timedelta(days=365)
                if (not ipo_date is None and vol_avg > self.filter_avg_volume and ipo_date < one_year_ago and (country not in FILTER_COUNTRY_EXCLUSION) and is_actively_trading):


                    #cpfr = fmp.financial_ratios(FMP_API_KEY, tic, period='annual', limit=500)

                    cpfg = fmp.financial_growth(FMP_API_KEY, tic, period='annual',limit=3)
                    for i in range(len(cpfg)):
                        date_f = cpfg[i]['date']

                        hpl = fmp.historical_price_full(FMP_API_KEY,tic,to_date=date_f)

                        last_adjusted_close = hpl[0]['adjClose']
                        last_volume = hpl[0]['volume']
                        self.db.upsert_stock_selection_fundamental_datas(date_f, result['isin'], result['symbol'], result['companyName'],  datetime.strptime(
                             result['ipoDate'],
                                '%Y-%m-%d').date(),ipo_date_lt_10_years, result[
                                 'exchangeShortName'], result['sector'], result['industry'],cpfg[i]['revenueGrowth'],cpfg[i]['ebitgrowth'],cpfg[i]['operatingIncomeGrowth'],cpfg[i]['netIncomeGrowth'],                                                                 cpfg[i]['epsgrowth'],cpfg[i]['inventoryGrowth'],
                                                                 cpfg[i]['rdexpenseGrowth'],cpfg[i]['debtGrowth'],
                                                                 cpfg[i]['sgaexpensesGrowth'], last_adjusted_close, last_volume)


                        df = pd.concat([df, pd.DataFrame.from_records([{
                            'date' : date_f,
                            'isin': result['isin'],
                            'tic': result['symbol'],
                            'stock_name': result['companyName'],
                            'ipo_date': datetime.strptime(
                                result['ipoDate'],
                                '%Y-%m-%d').date(),
                            'ipo_date_lt_10_years': ipo_date_lt_10_years,
                            'exchange_short_name': result[
                                'exchangeShortName'],
                            'sector': result['sector'],
                            'industry': result['industry'],
                            'revenue_growth' : cpfg[i]['revenueGrowth'],
                            'ebit_growth' : cpfg[i]['ebitgrowth'],
                            'operating_income_growth' : cpfg[i]['operatingIncomeGrowth'],
                            'net_income_growth' : cpfg[i]['netIncomeGrowth'],
                            'eps_growth' : cpfg[i]['epsgrowth'],
                            'inventory_growth' : cpfg[i]['inventoryGrowth'],
                            'rdexpense_growth' : cpfg[i]['rdexpenseGrowth'],
                            'debt_growth' : cpfg[i]['debtGrowth'],
                            'sgaexpenses_growth' : cpfg[i]['sgaexpensesGrowth'],
                            'last_adjusted_close' : last_adjusted_close,
                            'last_volume' : last_volume

                        }])])


            except Exception as ex:
                self.logger.exception(ex)
        df.reset_index(inplace=True, drop=True)
        df.sort_values('date')
        df['y_return'] = np.log(df['last_adjusted_close'].shift(-1) / df['last_adjusted_close'])

        self.db.close()
        print(df)

        return selected_stocks

if __name__ == '__main__':
    s = StockScreener(filter_exchange_list=FILTER_EXCHANGE_LIST,filter_min_price=FILTER_MIN_PRICE,filter_max_price=FILTER_MAX_PRICE,filter_avg_volume=FILTER_AVG_VOLUME, filter_keep_etf=FILTER_KEEP_ETF, filter_keep_fund=FILTER_KEEP_FUND)
    s.select_stocks()