import time

from myPackage import crawler
from myPackage import mongoServer as mon
from myPackage import write_to_csv as wcsv


def main():  # redis subscriber index:0-3
    client = mon.mongo_connection('linode1', 'mongo')
    while True:
        # 決定subscriber要取用哪筆資料
        stock_id = '2228'
        print(f"get stock {stock_id}")
        coll_stock = mon.mongo_collection(
            client, 'test', f"stock{stock_id}")
        for year in range(2010, 2021):
            # 測試當年度是否有資料
            test_month = 12
            test_url = f"""https://www.twse.com.tw/exchangeReport/STOCK_DAY?response=html&date={str(year)}{str(test_month).zfill(2)}01&stockNo={stock_id}"""
            print(f"test stock {stock_id} in {year} exist ?")
            test_docs = crawler.crawler(test_url)
            if test_docs:
                print("=> Yes, exist!")
                for month in range(1, 13):
                    url = f"""https://www.twse.com.tw/exchangeReport/STOCK_DAY?response=html&date={str(year)}{str(month).zfill(2)}01&stockNo={stock_id}"""
                    print(f"-- Crawler >>> {url}")
                    documents = crawler.crawler(url)
                    if documents:
                        # print(documents)
                        for item in documents:
                            # 記錄爬取的股票資料並寫入mongo
                            mon.insert_document(coll_stock, item)
                        print(
                            f'stock: {stock_id} in {year}{month} insert done.')
                    time.sleep(10)
                    print(f'stock: {stock_id} in {year}{month} crawl done.')
            print(f'stock: {stock_id} in {year} crawl done.')


if __name__ == '__main__':
    main()
