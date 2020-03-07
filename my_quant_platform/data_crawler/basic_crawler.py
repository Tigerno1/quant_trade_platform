import sys, os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from error_handler import DataValueError, DataCrawlError, DataParseError
from basic_util.db_conn import collection_dict
from data_handler.data_query import DataQuery
import tushare as ts 
from pymongo import UpdateOne, DESCENDING
from datetime import datetime, timedelta
import traceback 
import numpy



class BasicCrawler:

    def __init__(self):
        self.DataQuery = DataQuery()

    def crawl_new_stocks(self):
        df_stocks = ts.new_stocks()

        update_list = []
        for index in df_stocks.index:
            raw = dict(df_stocks.loc[index])
            doc = {
                'code': raw['code'],
                'ipo_date': raw['issue_date'],
                'price': raw['price']
            }

            update_list.append(UpdateOne(
            
            {'code': doc['code']}, {'$set': doc}, upsert=True))

        if len(update_list) > 0:
            update_db = collection_dict['NEW_STOCK_COLLECTION'][1].bulk_write(update_list, ordered=False)
            print('update new stocks, --> [collection: %s] inserted：%4d, modified：%4d' %
                  (collection_dict['NEW_STOCK_COLLECTION'][0], update_db.upserted_count, update_db.modified_count), flush=True)


    def crawl_basic(self, begin_date=None, end_date=None):
        '''
        crawl stock basic information during a period
        :param begin_date: begin date
        :param end_date: end date
        '''
        
        if begin_date is None:
            begin_date = (datetime.now()-timedelta(days=1)).strftime('%Y-%m-%d')
        if end_date is None:
            end_date = (datetime.now()-timedelta(days=1)).strftime('%Y-%m-%d')
        if datetime.timestamp(datetime.strptime(begin_date, '%Y-%m-%d')) > datetime.timestamp(datetime.strptime(end_date, '%Y-%m-%d')):
            raise DataValueError('The ending date must be set latter than the begining date !')

        all_dates = self.DataQuery.get_all_dates(begin_date, end_date)

        for date in all_dates:
            try:
                if self.crawl_basic_info(date):
                    raise DataCrawlError('Basic stock data cannot be found in API ! ', param_dict={'date': date})        
            except Exception as e:
                print(e)
                traceback.print_exc(limit=1, file=sys.stdout)
                continue
        
          
    
    def crawl_basic_info(self, date):
        df_basic = ts.get_stock_basics(date)
        if df_basic.empty:
            return
        update_list = []
        codes = set(df_basic.index)
        
        for code in codes:
            doc = dict(df_basic.loc[code])
           
            try:
                doc = self.fix_basic_data(code, date, doc)
                if doc:
                    update_list.append(
                        UpdateOne(
                            {'code': doc['code'], 'date': doc['date']},
                            {'$set': doc},
                            upsert=True
                        ))
                if len(update_list) > 0:
                    update_db = collection_dict['BASIC_COLLECTION'][1].bulk_write(update_list, ordered=False)
                    print('Get stock basic info --> [collection: %s] date: %s, inserted: %s, modified: %s' 
                     %(collection_dict['BASIC_COLLECTION'][0], date, update_db.upserted_count, update_db.modified_count), flush=True)
                else:
                    raise DataParseError('Error occur when try to fix data !', {'code': code, 'date': date})
            except Exception as e:
                print(e)
                traceback.print_exc(limit=1, file=sys.stdout)
                continue
           
            
            
        
    def fix_basic_data(self, code, date, doc):
        
        time_to_market = datetime.strptime(
                    str(doc['timeToMarket']), '%Y%m%d').strftime('%Y-%m-%d')

        totals = float(doc['totals'])

        outstanding = float(doc['outstanding'])

        doc.update({
            'code': code, 'date': date, 'timeToMarket': time_to_market, 
                'outstanding': outstanding, 'totals': totals
                })
        
        return doc
        



if __name__ == '__main__':
    bc = BasicCrawler()
#     # bc.crawl(begin_date='2017-01-01',  end_date='2017-12-31')
    # bc.crawl_basic(begin_date='2018-01-01',  end_date='2018-01-31')
    bc.crawl_basic(begin_date='2018-01-01',  end_date='2018-01-31')

  