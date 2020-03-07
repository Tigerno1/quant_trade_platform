
import sys, os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from settings.my_quant_platform_conf import CONFIG
from error_handler import CollectionConnectionError
from pymongo import MongoClient

class DbConn:
    def __init__(self):
        self.DATABASE = CONFIG.DATABASE
        self.DB_CONN = MongoClient(self.DATABASE.LOCAL_ADDRESS)[self.DATABASE.DATABASE_NAME]
        self.collection_name_dict = CONFIG.DATABASE.COLLECTION

    def make_conn_dict(self):
        collection_dict = {}
        for key, collection_name in self.collection_name_dict.items():
            try: 
                collection = self.DB_CONN[collection_name]
                if not collection:
                    raise CollectionConnectionError('cannot connect to collection: ', collection_name)
                collection_dict.update({key: (collection_name, collection)})
            except Exception as e:
                print(e)
        
        return collection_dict
    

# if __name__ == '__main__':

db = DbConn()
collection_dict = db.make_conn_dict()
