import datetime
import time

from myPackage import crawler
from myPackage import mongoServer as mon
from myPackage import stock_googlebot as goo
from myPackage import write_to_csv as wcsv

"""
crontab everyday 16:00
"""


def crawler_daily():
    # notify daily updation starts
    goo.main('stock_crawler', 'Stocks Daily Updation Starts!')
    t1 = datetime.datetime.now()
    client = mon.mongo_connection('linode1', 'mongo')
    coll_stockInfo = mon.mongo_collection(client, 'stocks', 'stockInfo')
    stocks = coll_stockInfo.find({}, {'_id': 1})
    # set daily status zero for default
    coll_stockInfo.update_many({}, {'$set': {'dailyStatus': 0}})
    today = datetime.date.today()-datetime.timedelta(1)
    year = today.strftime("%Y")
    month = today.strftime("%m")
    day = today.strftime("%d")
    # get all stocks' id
    for stock in stocks:
        retry = 0
        stock_id = stock['_id']
        url = f"""https://www.twse.com.tw/exchangeReport/STOCK_DAY?response=html&date={year}{month}01&stockNo={stock_id}"""
        coll_stock = mon.mongo_collection(client, 'stocks', f"stock{stock_id}")
        # print(url)
        while retry < 3:
            try:
                contents = crawler.crawler(url)
                print(contents)
                for item in contents:
                    # daily record to mongo
                    mon.insert_document(coll_stock, item)
                    # crawlering and writing to mongo done, set daily status as 1
                    coll_stockInfo.update_one(
                        {'_id': stock_id}, {'$set': {'dailyStatus': 1}})
                    time.sleep(10)
                    break
            except Exception as e:
                print(e)
                time.sleep(10)
                retry += 1
                if retry == 3:
                    # sent notify with googlebot
                    goo.main('stock_crawler',
                             f"{stock_id}, {year,month,day} Wrong: {e}")
                    wcsv.writeToCsv('DailyCrawlerException', [
                                    stock_id, year, month, day])
                continue
    # notify daily updation done
    cost_time = datetime.datetime.now() - datetime.datetime.now()
    goo.main('stock_crawler',
             f"{datetime.date.today()}: Daily Updation Finished!\nCost_time: {cost_time}")
    return


if __name__ == '__main__':
    conts = crawler_daily()
