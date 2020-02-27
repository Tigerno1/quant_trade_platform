
import tushare as ts
from pymongo import MongoClient, UpdateOne
from datetime import datetime

class DataCrawler:
    def __init__(self):
        self.db = MongoClient('mongodb://127.0.0.1:27017')['my_quant']

     
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
        :param autype: autype = 'qfq' --> return price of split adjusted price
        autype = 'hfq' --> return back rehabilitation price
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
            df_data = ts.get_k_data(code, index=False, autype=autype, start=begin_date, end=end_date)
            self.save_data(code, df_data, autype=autype, index=False)

    def save_data(self, code, data, autype=None, index=False):
        '''
        get stock information from tushare and save to mongodb
        :param codes: stock_list
        :param autype: rehabilitition type
        :param begin_date: begine date
        :param end_date: end date
        '''

        update_requests = []
        for df_index in data.index:
            doc = dict(data.loc[df_index])
            doc['code'] = code
            doc['index'] = index

            update_requests.append(
                UpdateOne(
                    {'code': doc['code'], 'date': doc['date'], 'index': index},
                     {'$set': doc}, upsert=True))

        
        if len(update_requests) > 0:
            collection_name = 'daily_' + autype if autype else 'daily'
            update_result = self.db[collection_name].bulk_write(update_requests, ordered=False)

            print('save data--> collection: %s, code: %s, inserted: %4d, modified: %4d'
            %(collection_name, code, update_result.upserted_count, update_result.modified_count), flush=True)

if __name__ == '__main__':
    dc = DataCrawler()
    # dc.crawl_stock(autype='qfq', begin_date='2015-01-1', end_date='2015-01-15')
    dc.crawl_index(begin_date='2018-01-01', end_date='2018-01-31')
    # dc.crawl_stock(begin_date='2018-01-01', end_date='2018-01-31')
    # dc.crawl_stock(autype='hfq', begin_date='2015-01-01', end_date='2015-01-31')






