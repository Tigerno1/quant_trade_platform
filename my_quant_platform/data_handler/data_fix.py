import sys, os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from basic_util.db_conn import collection_dict
from pymongo import MongoClient, ASCENDING, UpdateOne
import tushare as ts
from datetime import datetime, timedelta
from data_handler.data_storage import DataStorage
from data_handler.data_query import DataQuery 
from error_handler import DataQueryError, DataValueError
import threading

class DailyFixing:
    def __init__(self, begin_date, end_date):
        self.begin_date = begin_date
        self.end_date = end_date
        self.DataQuery = DataQuery()
        self.all_dates = self.DataQuery.get_all_dates(self.begin_date, self.end_date)
        # self.all_codes = self.DataQuery.get_all_codes()
        self.all_codes = ['002663', '002342']
        self.DataStorage = DataStorage()
        # self.data_bases = ['daily', 'daily_hfq', 'daily_qfq']
        self.collection_dict = collection_dict
        self.collection_list = [self.collection_dict['DAILY_COLLECTION'], self.collection_dict['B_DAILY_COLLECTION']]
    
    def fill_is_trading(self, date=None):
        if date is None:
            all_dates = self.all_dates
        else:
            all_dates = [date]
        for date in all_dates:
            try:
                self.fill_single_date_is_trading(date=date, collection=self.collection_dict['DAILY_COLLECTION'])
                self.fill_single_date_is_trading(date=date, collection=self.collection_dict['B_DAILY_COLLECTION'])
            except Exception as e:
                print(e)
                continue
        
    def fill_single_date_is_trading(self, date=None, collection=None):
        projection = {'code': True, 'volume':True, '_id': False}
        daily_code_cursor = self.DataQuery.code_cursor(date=date, index=False, collection=collection, projection=projection)
                                      
        def set_update_dict(data = None, inner_func_param=None):
            if not data.__contains__('is_trading'):
                update_dict = {'is_trading': (data['volume'] > 0)}
            else:
                update_dict = None
            return update_dict

        self.DataStorage.save_data(date=date, data_loop=daily_code_cursor, collection=collection, inner_func = set_update_dict, message='is_trading')


    def fill_is_suspended(self):
        for code in self.all_codes:
            for collection in self.collection_list:
                projection={'date': True, 'close': True, '_id': False}
                try:
                    daily_date_cursor = self.DataQuery.date_cursor(code=code, begin_date=self.begin_date, end_date=self.end_date, index=False, collection=collection, projection=projection)
                    daily_date_dict = self.DataQuery.dict_date_data(daily_date_cursor)
                except Exception as e:
                    print(e)
                    continue

                def set_update_dict(code=None, date=None, data_loop=None, inner_func_param=None, last_daily=[]):
                    suspension_daily = None
                    if date in inner_func_param:
                        last_daily.append(inner_func_param[date])
                        
                    else:
                        if last_daily:
                            daily = last_daily.pop(-1)
                            suspension_daily = {
                                'code': code,
                                'date': date,
                                'is_trading': False,
                                'index': False,
                                'volume': 0,
                                'open': daily['close'],
                                'close': daily['close'],
                                'high': daily['close'],
                                'low': daily['close']}

                    return suspension_daily
            
                self.DataStorage.save_data(code=code, date=None, index=False, collection=collection,  data_loop=self.all_dates, inner_func=set_update_dict, inner_func_param=daily_date_dict, message='is_suspended')
                       

    def fill_aufactor(self):
        for date in self.all_dates:
            projection ={'code': True, 'close': True, '_id': False}
            try:
                code_daily_cursor = self.DataQuery.code_cursor(date=date, index=False, collection=collection_dict['DAILY_COLLECTION'], projection=projection)
                code_daily_dict = self.DataQuery.dict_code_data(code_daily_cursor)
                code_daily_hfq_cursor = self.DataQuery.code_cursor(date=date, index=False, collection=collection_dict['B_DAILY_COLLECTION'], projection=projection)

            except Exception as e:
                print(e)
                continue
                

            def set_update_dict(data=None, inner_func_param=None):
                if data.__contains__('code'):
                    code = data['code']
                if code in inner_func_param and inner_func_param[code]['close'] != 0:
                    aufactor = data['close']/inner_func_param[code]['close']
                    aufactor = round(aufactor, 3)  
                    
                else: 
                    aufactor = None

                return {'aufactor': aufactor}
                
            self.DataStorage.save_data(date=date, index=False, collection=collection_dict['DAILY_COLLECTION'], data_loop=code_daily_hfq_cursor, inner_func=set_update_dict, inner_func_param = code_daily_dict, message='fill_aufactor')

    def fill_pre_close(self):
        for code in self.all_codes:
            projection={'code': True, 'date': True, 'close': True, 'volume':True, 'aufactor': True, 'is_trading': True}
            try:
                date_daily_cursor = self.DataQuery.date_cursor(
                    code=code, begin_date=self.begin_date, end_date=self.end_date,
                    index=False, collection=collection_dict['DAILY_COLLECTION'], 
                    projection=projection
                    )
            except Exception as e:
                print(e)
                continue

            date_daily_dict = self.DataQuery.dict_date_data(date_daily_cursor)
            try:
                if date_daily_dict.__contains__(self.end_date):
                    if date_daily_dict[self.end_date].__contains__('aufactor'):
                        aufactor_date = self.end_date
                        pre_aufactor = date_daily_dict[self.end_date]['aufactor']
                    
                    else: 
                        aufactor_date = self.DataQuery.get_trading_date_before(self.end_date, 1)
                        if date_daily_dict[aufactor_date].__contains__('aufactor'):
                            pre_aufactor = date_daily_dict[aufactor_date]['aufactor']
                else:
                    raise DataValueError('Cannot find the right data to calculate aufactor')
            except Exception as e:
                print(e)
                continue
                    
           
            def set_update_dict(code=None, date=None, data_loop=None, inner_func_param=None):
                
                if data_loop.__contains__(date) and data_loop[date].__contains__('aufactor'):
                    pre_close = data_loop[date]['close']*data_loop[date]['aufactor']/inner_func_param
            
                    pre_volume = data_loop[date]['close']*data_loop[date]['aufactor']/inner_func_param

                    pre_close = round(pre_close, 3)
                    pre_volume = int(pre_volume)
                    
                    update_dict = {'pre_close': pre_close, 'pre_volume': pre_volume}
                else: update_dict = None

                return update_dict 
                
            self.DataStorage.save_data(code=code, index=False, collection=collection_dict['DAILY_COLLECTION'], data_loop=date_daily_dict, inner_func=set_update_dict, inner_func_param=pre_aufactor, message='fill_pre_close')

if __name__ == '__main__':
    df = DailyFixing(begin_date='2018-01-01', end_date='2018-01-31')
    df.fill_is_trading()
    # df.fill_is_suspended()
    df.fill_aufactor()
    # df.fill_pre_close()