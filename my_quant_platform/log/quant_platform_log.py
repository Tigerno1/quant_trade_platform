import sys, os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import logging, logging.handlers
from quant_platform_config import LOG_PATH


class QuantTradeLog:
    def __init__(self, name):

        #set format for all logs
        formatter = logging.Formatter(
            fmt='[%(asctime)s] - [%(name)s] - [%(levelname)s] - [%(module)s]: %(message)s'
        )
         # set message log 
        self.logger = logging.getLogger(name)
        self.logger.setLevel(logging.INFO)     
       
        file_path = os.path.join(LOG_PATH, name + '.log')
        print(file_path)
        handler = logging.handlers.TimedRotatingFileHandler(
            file_path, when='D', interval=1, encoding='utf-8'
        )
        handler.setFormatter(formatter)
        self.logger.addHandler(handler)

        # set error log
        self.error_logger = logging.getLogger('error')
        self.error_logger.setLevel(logging.ERROR)
        error_file_path = os.path.join(LOG_PATH, 'error.log')
        error_handler = logging.handlers.TimedRotatingFileHandler(file_path, when='D', interval=1, encoding='utf-8')
        error_handler.setFormatter(formatter)
        self.error_logger.addHandler(error_handler)

        #set profile log
        self.profile_logger = logging.getLogger('profile')
        self.profile_logger.setLevel(logging.INFO)
        profile_file_path = os.path.join(LOG_PATH, 'profile.log')
        profile_handler = logging.handlers.TimedRotatingFileHandler(file_path, when='D', interval=1, encoding='utf-8')
        profile_handler.setFormatter(formatter)
        self.profile_logger.addHandler(error_handler)


    def log(self, message, *args):
        '''
        normal log
        :param message: information
        '''
        self.logger.info(message, *args)
        

    def profile(self, message):
        '''
        normal log
        :param message: information
        '''
        self.profile_logger.info(message)

    def warning(self, message, *args):
        '''
        normal log
        :param message: information
        '''
        self.logger.warning(message, args)

    def error(self, message):
        '''
        normal log
        :param message: info
        '''
        self.error_logger.exception(message, exc_info=True)


if __name__ == '__main__':
    quant_log = QuantTradeLog('test')
    quant_log.log('This is message')
