# _*_ coding: utf-8 _*_

import sys, os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from basic_util.db_conn import collection_dict
import tushare as ts
from pymongo import MongoClient, UpdateOne
from datetime import datetime
from error_handler import DataInputError, DataCrawlError
import threading

class DataCrawler:
    def __init__(self):
        self.collection_dict = collection_dict
        

    def crawl_index(self, begin_date=None, end_date=None):
        '''
        get index info through index code
        :param begin_date: begin date
        :param end_date: end date
        '''
        codes = ('000001', '000300', '399001', '399005', '399006')
        # codes = ['000001']
        if begin_date is None:
            begin_date = '2010-01-01'
        
        if end_date is None:
            end_date = datetime.now().strftime('%Y-%m-%d')
        
        for code in codes:
            df_data = ts.get_k_data(code, index=True, start=begin_date, end=end_date)
            self.save_data(code, df_data, autype=None, index=True)


    def crawl_stock(self, autype=None, begin_date=None, end_date=None):
        '''
        get stock info through index code
        :param autype: autype = 'for' --> return price of split adjusted price
        autype = 'back' --> return back rehabilitation price
        default: autype=None --> return real price
        :param begin_date: begin date 
        :param end_date: end date
        :return: list of stock information
        '''
        df_stock = ts.get_stock_basics()
        codes = list(df_stock.index)

        if begin_date is None:
            begin_date = '2010-01-01'
        
        if end_date is None:
            end_date = datetime.now().strftime('%Y-%m-%d')
        

        # codes = ['000001', '600000']
        for code in codes:
            try:
                df_data = ts.get_k_data(code, start=begin_date, end=end_date, index=False, autype=autype)
                if df_data.empty:
                    raise DataCrawlError('Cannot crawl data from API')
            except Exception as e:
                print(e)

            self.save_data(code, df_data, autype=autype, index=False)

    def save_data(self, code, data, autype=None, index=False):
        '''
        get stock information from tushare and save to mongodb
        :param codes: stock_list
        :param autype: rehabilitition type
        :param begin_date: begine date
        :param end_date: end date
        '''

        update_list = []
        for df_index in data.index:
            doc = dict(data.loc[df_index])
            doc['code'] = code
            doc['index'] = index

            update_list.append(
                UpdateOne(
                    {'code': doc['code'], 'date': doc['date'], 'index': doc['index']},
                     {'$set': doc}, upsert=True))
            

        if len(update_list) > 0:
            if autype == 'qfq': 
                collection = self.collection_dict['F_DAILY_COLLECTION']
            elif autype == 'hfq':
                collection = self.collection_dict['B_DAILY_COLLECTION']
            elif autype is None:
                collection = self.collection_dict['DAILY_COLLECTION']
            else:
                raise DataInputError('The autype you put in is wrong; The system only support qfq, hfq and None in string format !', autype)

        update_db = collection[1].bulk_write(update_list, ordered=False)
        print('save data--> [collection: %s], code: %s, inserted: %4d, modified: %4d'
        %(collection[0], code, update_db.upserted_count, update_db.modified_count), flush=True)

    
    def run(self, autype=None, begin_date=None, end_date=None):
        try:
            crawl_index = threading.Thread(target=self.crawl_index, args=(begin_date, end_date))
            crawl_stock = threading.Thread(target=self.crawl_stock, args=(begin_date, end_date))
            crawl_f_stock = threading.Thread(target=self.crawl_stock, args=('qfq', begin_date, end_date))
            crawl_b_stock = threading.Thread(target=self.crawl_stock, args=('hfq', begin_date, end_date))
            crawl_index.start()
            crawl_stock.start()
            crawl_f_stock.start()
            crawl_b_stock.start()
        except Exception as e:
            print(e)
        
        print("Crawling daily data has finished !")




if __name__ == '__main__':
    dc = DataCrawler()
    # dc.crawl_index(begin_date='2018-01-01', end_date='2018-01-31')
    # dc.crawl_stock(begin_date='2018-01-01', end_date='2018-01-31')
    # dc.crawl_stock(autype='hfq', begin_date='2018-01-01', end_date='2018-01-31')
    # dc.crawl_stock(autype='qfq', begin_date='2018-01-1', end_date='2018-01-15')





