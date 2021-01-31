from myPackage import crawler
from myPackage import mongoServer as mon
import time


def crawler_single_stock(stock_id):
    mongoClient = mon.mongo_connection('linode1', 'mongo')
    coll_stocks = mon.mongo_collection(mongoClient, 'test', f'stock{stock_id}')
    for year in range(2010, 2021):
        for month in range(1, 13):
            url = f"""https://www.twse.com.tw/exchangeReport/STOCK_DAY?response=html&date={str(year)}{str(month).zfill(2)}01&stockNo={stock_id}"""
            print(url)
            docs = crawler.crawler(url)
            # print(docs)
            for doc in docs:
                print(doc)
                mon.insert_document(coll_stocks, doc)
            time.sleep(10)
    return


if __name__ == '__main__':
    stock_id = '2330'
    crawler_single_stock(stock_id)