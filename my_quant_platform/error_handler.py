import sys, os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


class DefaultError(Exception):
    def __init__(self, message, param_dict=None):
        self.message = message
        self.param_dict = param_dict

    def __str__(self):
        param_str = ''
        if self.param_dict:
            for param_key, param_value in self.param_dict.items():
                if len(param_key) > 1:
                    param_str += str(param_key) + ': ' + str(param_value) + ', '
                else: 
                    param_str += str(param_key) + ': ' + str(param_value)

        
        return (str(self.message) + param_str + str('. '))




# Handle Date Errors

class DateValueError(Exception):
    pass

class DateQueryError(Exception):
    pass

#Handle Data Errors

class DataStorageError(Exception):
    pass

class DataQueryError(Exception):
    pass

class DataValueError(Exception):
    pass




#Handle crawl Error
class DataCrawlError(DefaultError):
    pass

class DataParseError(DefaultError):
    pass

class DataInputError(DefaultError):
    pass


class CollectionConnectionError(DefaultError):
    pass






    

# if __name__ == '__main__':
#     try:
#         raise DataStorageError('error')
#     except DataStorageError as e:
#         print(e)

