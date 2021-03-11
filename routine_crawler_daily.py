import datetime
import time

from myPackage import all_stock_id as allStockID
from myPackage import crawler
from myPackage import mongoServer as mon
from myPackage import stock_googlebot as goo
from myPackage import write_to_csv as wcsv

"""
crontab everyday 16:30
"""


def crawler_daily():
    counts = 0
    # notify daily updation starts
    goo.main('stock_crawler', 'Stocks Daily Updation Starts!')
    # start time
    t1 = datetime.datetime.now()
    # set daily status zero for default
    client = mon.mongo_connection('linode1', 'mongo')
    coll_stockInfo = mon.mongo_collection(client, 'stocks', 'stockInfo')
    coll_stockInfo.update_many({}, {'$set': {'dailyStatus': 0}})
    # today
    today = datetime.date.today() #-datetime.timedelta(1)
    year = today.strftime("%Y")
    month = today.strftime("%m")
    day = today.strftime("%d")
    # get all stocks' id
    for content in allStockID.all_stock_id():
        stock_id = content['_id']
        print(stock_id)
        retry = 0
        url = f"""https://www.twse.com.tw/exchangeReport/STOCK_DAY?response=html&date={year}{month}01&stockNo={stock_id}"""
        coll_stock = mon.mongo_collection(client, 'stocks', f"stock{stock_id}")
        while retry < 3:
            try:
                contents = crawler.crawler(url)
                # print(contents)
                for item in contents:
                    # daily record to mongo
                    mon.insert_document(coll_stock, item)
                # crawlering and writing to mongo done, set daily status as datetime
                coll_stockInfo.update_one(
                    {'_id': stock_id}, {'$set': {'dailyStatus': f"{year+month+day}"}})
                counts += 1
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
                    wcsv.writeToCsv(f'./data/DailyCrawlerException_{today}', [
                                    stock_id, year, month, day])
                continue

    # check daily update done
    if coll_stockInfo.find({'dailyStatus': {'$ne': f"{year+month+day}"}}, {'_id': 1}).count() != 0:
        crawler_daily()

    # notify daily updation done
    cost_time = datetime.datetime.now() - t1
    goo.main('stock_crawler',
             f"{datetime.date.today()}: Daily Updation Finished!\nCheck amount of stock: {counts}, except: {938-counts}\nCost_time: {cost_time}")
    return


if __name__ == '__main__':
    crawler_daily()
