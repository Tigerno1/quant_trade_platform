import sys, os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from basic_util.db_conn import collection_dict
from pymongo import UpdateOne
from error_handler import DataStorageError, DataValueError

class DataStorage:
    def __init__(self):
        pass


    def save_data(self, code=None, date=None, index=False, collection=None, data_loop=None, inner_func=None, inner_func_param=None, upsert=False, message=None):
        
        update_list = []

        for data in data_loop:
            if code:
                date = data
                data_set = inner_func(code=code, date=date, data_loop=data_loop, inner_func_param=inner_func_param)
                if data_set:
                    update_list.append(
                        UpdateOne(
                            {'code': code, 'date': date, 'index': False},
                            {'$set': data_set},
                                upsert=True))

            else: 
                
                data_set = inner_func(data=data, inner_func_param=inner_func_param)
                if data_set:
                    update_list.append(
                    UpdateOne(
                        {'code': data['code'], 'date': date, 'index': False},
                        {'$set': data_set}))
                    
            
        if len(update_list) > 0:
            update_db = collection[1].bulk_write(update_list, ordered=False)
            print('Update %s --> [collection]: %s, date: %s, inserted:%4d, modified: %4d'\
                % (message, collection[0], date, update_db.upserted_count, update_db.modified_count), flush=True)

       





