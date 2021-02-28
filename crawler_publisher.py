import time

from myPackage import mongoServer as mon
from myPackage import redisServer as red
from myPackage import write_to_csv as wcsv

"""
In url public-subscribe system, use db=0 in redis
"""


def main():
    client = mon.mongo_connection('linode1', 'mongo')
    coll_stockInfo = mon.mongo_collection(client, 'stocks', 'stockInfo')
    redisConnect = red.redis_connection('linode1', 'redis', db=0)
    while True:
        try:
            # check if key in redis doesn't exist
            for num in range(1, 9):
                key = f'stock_No_{num}'
                if not redisConnect.exists(key):
                    # 表示還有未丟到redis的stock_id
                    if coll_stockInfo.find({'crawlerStatus': 0}, {'_id': 1}).count() != 0:
                        content = coll_stockInfo.find(
                            {'crawlerStatus': 0}, {'_id': 1}).limit(1)
                        stock_id = content[0]['_id']
                        print(f"{key} disapear >>> set {stock_id}")
                        # 放到redis
                        red.redis_set_key_value(redisConnect, key, stock_id)
                        # 表示已經放到redis
                        coll_stockInfo.update({'_id': stock_id}, {
                            '$set': {'crawlerStatus': 1}})
                    # 表示已經從redis刪掉但還沒爬蟲好的stock_id
                    elif coll_stockInfo.find({'crawlerStatus': {'$ne': 3}}, {'_id': 1}).count() != 0:
                        content = coll_stockInfo.find(
                            {'crawlerStatus': 1}, {'_id': 1}).limit(1)
                        stock_id = content[0]['_id']
                        print(f"{key} disapear >>> set {stock_id}")
                        # 再丟上去一次redis
                        red.redis_set_key_value(redisConnect, key, stock_id)
                    else:
                        break
                print(f"{key} still exist.")
            time.sleep(100)
            # 全部爬蟲完成就中止丟資料到redis
            if coll_stockInfo.find({'crawlerStatus': {'$ne': 3}}, {'_id': 1}).count() == 0:
                print("== All stock crawlering done ==")
                break
        except Exception as e:
            wcsv.writeToCsv("./data/redisException", [e])
            print(e)


if __name__ == '__main__':
    main()
