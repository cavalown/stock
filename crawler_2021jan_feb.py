import time

from crawler_subscriber import main
from myPackage import all_stock_id as allStockID
from myPackage import crawler
from myPackage import mongoServer as mon
from myPackage import write_to_csv as wcsv


def crawl_jan_feb():
    client = mon.mongo_connection('linode1', 'mongo')
    for content in allStockID.all_stock_id():
        stock_id = content['_id']
        for month in ['01', '02']:
            url = f"""https://www.twse.com.tw/exchangeReport/STOCK_DAY?response=html&date=2021{month.zfill(2)}01&stockNo={stock_id}"""
            coll_stock = mon.mongo_collection(
                client, 'stocks', f"stock{stock_id}")
            print(f"-- Crawler >>> {url}")
            documents = crawler.crawler(url)
            if documents:
                for item in documents:
                    # 記錄爬取的股票資料並寫入mongo
                    mon.insert_document(coll_stock, item)
            time.sleep(10)
            print(f'stock: {stock_id} in 2021{month.zfill(2)} crawl done.')
        print(f'stock: {stock_id} in finished Jan and Feb data crawlering.')


if __name__ == '__main__':
    crawl_jan_feb()
