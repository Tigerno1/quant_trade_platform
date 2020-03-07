#  -*- coding: utf-8 -*-
import sys, os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import json, traceback, urllib3
from data_handler.data_query import DataQuery
from pymongo import UpdateOne
from basic_util.db_conn import collection_dict
from error_handler import DataParseError, DataCrawlError, CollectionConnectionError

class FinanceReportCrawler:
    def __init__(self):
        self.url_report= 'http://dcfm.eastmoney.com/em_mutisvcexpandinterface/api/js/get?'\
                'type={1}&token=70f12f2f4f091e459a279469fe49eca5&'\
                'st=reportdate&sr=-1&p=1&ps=80&'\
                'js={"pages":(tp),"data":%20(x)}&'\
                'filter=(scode=%27{2}%27)&rt=51140869'\
        
        self.url_performance  = 'http://dcfm.eastmoney.com//em_mutisvcexpandinterface' \
              '/api/js/get?type=YJBB20_YJBB&token=70f12f2f4f091e4' \
              '59a279469fe49eca5&st=reportdate&sr=-1&filter=(scode={1})&' \
              'p=1&ps=100&js={"pages":(tp),"data":(x)}'

        self.user_agent = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_3)'\
             'AppleWebKit/537.36 (KHTML, like Gecko) Chrome/79.0.3945.130 Safari/537.36'

        self.all_codes = DataQuery().get_all_codes()
        # all_codes = ['600000']
        self.report_types = ['CWBB_ZCFZB', 'CWBB_XJLLB', 'CWBB_LRB', 'YJBB20_YJBB']
        # self.report_types = ['YJBB20_YJBB']
        self.collection_dict = collection_dict
        # report_types = ['CWBB_LRB']


    def parse_report(self, data, report_type):
        try:
            if report_type == 'YJBB20_YJBB':
                data = {
                'code': data['scode'],
                'name': data['sname'],
                'basic_eps': data['basiceps'],
                'report_date': data['reportdate'][0:10],
                'announced_date': data['latestnoticedate'][0:10]}


            else:
                data.update({
                    'announced_date': data['noticedate'][0:10],
                    'report_date': data['reportdate'][0:10],
                    'code': data['scode'],
                    'name': data['sname']
                }) 
    
            return data   
        except:
            raise DataParseError('Cannot parse the report crawled !')
        
        

    def save_report(self, data_loop, inner_func, report_type):
       
        update_list = []
        for data in data_loop:
            try:
                data_dict = self.parse_report(data, report_type)
            except Exception as e:
                print(e)
    
            if data_dict: 
                # print(data_dict)
                update_list.append(
                    UpdateOne(
                        {'code': data_dict['code'], 
                        'report_date': data_dict['report_date'],
                        'announced_date': data_dict['announced_date']},
                        {'$set': data_dict},
                        upsert=True))

                       
        if len(update_list) > 0:
            if report_type == 'CWBB_ZCFZB':
                collection = collection_dict['BALANCE_SHEET_COLLECTION']
            elif report_type == 'CWBB_XJLLB':
                collection = collection_dict['CASH_FLOW_COLLECTION']
            elif report_type == 'CWBB_LRB':
                collection = collection_dict['PROFIT_LOSS_COLLECTION']
            elif report_type == 'YJBB20_YJBB':
                collection = collection_dict['PERFORMANCE_REPORT_COLLECTION']
            else:
                raise CollectionConnectionError('Cannot store data in collection !')
            update_db = collection[1].bulk_write(update_list, ordered=False)
            print('Get financial report data --> [collection: %s], stock：%s, type：%10s, insert：%4d, update：%4d' 
            %(collection[0], data_dict['code'], report_type, update_db.upserted_count, update_db.modified_count),
                    flush=True)

    def crawl_report(self):
        conn_pool = urllib3.PoolManager()
        
        for report_type in self.report_types:
            for code in self.all_codes:
                if report_type == 'YJBB20_YJBB':
                    url = self.url_performance.replace('{1}', code)
                else: 
                    url = self.url_report.replace('{1}', report_type).replace('{2}', code)
                try:
                    response = conn_pool.request(
                        'GET',
                        url=url,
                        headers={'User-Agent': self.user_agent}
                    )
                    if response.data: 
                        result = json.loads(response.data.decode('utf-8'))
                        report_dict = result['data']
                        # print(report_dict)
                        self.save_report(data_loop=report_dict, inner_func=self.parse_report, report_type=report_type)
            
                    else:
                        raise DataCrawlError('Cannot crawl data from the web: East Money Information')
                except Exception as e:
                    print(e)
                    
                  
                    
if __name__ == '__main__':
    frc = FinanceReportCrawler()
    frc.crawl_report()




