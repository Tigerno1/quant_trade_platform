# import sys, os
# sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import os
import sys
import re
from collections import defaultdict

class ConfigGenerator(defaultdict):
    def __init__(self):
        super(ConfigGenerator, self).__init__(ConfigGenerator)

    def __getattr__(self, key):
        try:
            return self[key]
        except KeyError:
            raise AttributeError(key)

    def __setattr__(self, key, value):
        self[key] = value

# if __name__ == '__main__':
    
CONFIG = ConfigGenerator()

#PATH
CONFIG.PATH.MAIN_PATH = str(os.path.dirname(os.path.abspath(__file__)))
CONFIG.PATH.LOG_PATH = str(os.path.join(CONFIG.PATH.MAIN_PATH, 'log'))


#DATABASE

CONFIG.DATABASE.DATABASE_NAME = 'my_quant'
CONFIG.DATABASE.LOCAL_ADDRESS = 'mongodb://127.0.0.1:27017'


#COLLECTION

CONFIG.DATABASE.COLLECTION.DAILY_COLLECTION = 'daily'
CONFIG.DATABASE.COLLECTION.F_DAILY_COLLECTION = 'daily_qfq'
CONFIG.DATABASE.COLLECTION.B_DAILY_COLLECTION = 'daily_hfq'

#warnning: These two collection_names are created by autype + daily_collection_name in daily_crawler
#so the name cannot be altered manually. if changing is required, please change the name of 
# daily_collection_name, the above two collection name will be altered by autype + new collection name


# BASIC COLLECTION

CONFIG.DATABASE.COLLECTION.BASIC_COLLECTION = 'basic'
CONFIG.DATABASE.COLLECTION.NEWSTOCK_COLLECTION = 'new_stock'


# FINANCIAL REPORT COLLECTION

CONFIG.DATABASE.COLLECTION.BALANCE_SHEET_COLLECTION = 'balance_sheet'
CONFIG.DATABASE.COLLECTION.PROFIT_LOSS_COLLECTION = 'profit_loss'
CONFIG.DATABASE.COLLECTION.CASH_FLOW_COLLECTION = 'cash_flow'
CONFIG.DATABASE.COLLECTION.PERFORMANCE_REPORT_COLLECTION = 'performance'












