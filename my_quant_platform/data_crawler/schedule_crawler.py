import sys, os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from crawler.daily_crawler import DataCrawler
from crawler.basic_crawler import BasicCrawler
import time
from datetime import datetime
import schedule


def crawl_daily():
    dc = DataCrawler()
    bc = BasicCrawler()

    current_date = datetime.now().striftime('%Y-%m-%d')
    weekday = current_date.strftime('%w')
    if 0 < weekday < 6:
        #index
        dc.crawl_index(begin_date=current_date, end_date=current_date)
        #no adjustment share price 
        dc.crawl_stock(begin_date=current_date, end_date=current_date)
        #split-adjusted share prices
        dc.crawl_stock(autype='qfq', begin_date='2005-01-01', end_date=current_date)
        #back rehabilitation price
        dc.crawl_stock(autype='hfq', begin_date=current_date, end_date=current_date)
        bc.basic_crawl(current_date)

if __name__ == '__main__':
    schedule.every().day.at['15:30'].do(crawl_daily)

    while True:
        schedule.run_pending()
        time.sleep(5)