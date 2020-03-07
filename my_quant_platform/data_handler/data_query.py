import sys, os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from basic_util.db_conn import collection_dict
import time 
from datetime import datetime, timedelta
from pymongo import MongoClient, ASCENDING, DESCENDING
from error_handler import DataStorageError, DataQueryError, DataValueError, DateQueryError, DateValueError



class DataQuery:
    def __init__(self):
        pass

    def date_cursor(self, code=None, collection=None, index=False, begin_date=None, end_date=None, projection=None):
        
        if code is None:
            raise DateQueryError('Cursor cannot query data without code !')
        
        if collection is None:
            raise DataQueryError('Cannot query data without collection name !')

        if begin_date is None:
            begin_date = datetime.now().strftime('%Y-%m-%d')

        if end_date is None:
            end_date = datetime.now().strftime('%Y-%m-%d')

        if time.mktime(time.strptime(begin_date, '%Y-%m-%d')) > time.mktime(time.strptime(end_date, '%Y-%m-%d')):
            raise DateValueError('The ending date must be set latter than the begining date !')
        
            
        date_daily_cursor = collection[1].find(
            {'code': code, 'date': {'$gte': begin_date, '$lte': end_date}, 
                'index': index},
            sort=[('date', ASCENDING)],
            projection=projection
            ) 
        
        if not date_daily_cursor.count():         
            raise DataQueryError('Cannot find the data you want to query !')

        return date_daily_cursor
            

    def get_all_dates(self, begin_date=None, end_date=None):

        try:
            projection = {'date': True, '_id': False}
            daily_index_dict = self.date_cursor(code='000001', begin_date=begin_date, end_date=end_date, index=True, collection=collection_dict['DAILY_COLLECTION'], projection=projection)
            dates = [daily_index['date'] for daily_index in daily_index_dict]
            return dates
        except Exception as e:
            print(e)

    def get_trading_date_before(self, date, days): 
        count = days + 1
        daily_cursor = collection_dict['DAILY_COLLECTION'][1].find(
            {'code':'000001', 
            'date': {'$lte': date},
            'index': True},
            sort=[('date', DESCENDING)],
            projection={'date': True, '_id': False},
            limit=count
        )

        dates = [daily['date'] for daily in daily_cursor]

        if len(dates) == count:
            return dates[days]

        return None
    


    def code_cursor(self, date=None, index=None, collection=None, projection=None):

        if date is None:
            date = datetime.now().strftime('%Y-%m-%d')
        
        if collection is None: 
            raise DateQueryError('Cannot query data without collection name !')

        if index is None:
            code_daily_cursor = collection[1].find(
                {'date': date},
                projection = projection,
                batch_size = 1000
                )
            
        else:
            code_daily_cursor = collection[1].find(
                {'date': date, 'index': index},
                projection=projection,
                batch_size = 1000
                )

        if not code_daily_cursor.count():         
            raise DataQueryError('Cannot find the data you want to query !')
        
        return code_daily_cursor


    def get_all_codes(self, date=None):
    
        '''
        get stock code list
        1. if set dates, return the stock codes of that day
        2. if not, return all stock codes 
        :param date: date
        return: stock code list.
        '''
        if date is not None:
            codes = self.code_cursor(date=date, collection=collection_dict['BASIC_COLLECTION'])
            
        else:
            codes = collection_dict['BASIC_COLLECTION'][1].distinct('code')

            return codes

    def dict_code_data(self, code_cursor):
        return dict([(code_daily['code'], code_daily) for code_daily in code_cursor])

    def dict_date_data(self, date_cursor):
        return dict([(date_daily['date'], date_daily) for date_daily in date_cursor])

        


        


# if __name__ == '__main__':
#     df = DataQuery()
#     rs = df.get_all_dates('2018-01-01', '2018-01-31')
#     print(rs)
    # rs = df.code_cursor(date='2015-01-08', collection='daily', index=False, projection={'code': True})
    # for i in rs:
    #     print(i['code'])
