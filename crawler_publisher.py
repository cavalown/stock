import time
from myPackage import mongoServer as mon
from myPackage import redisServer as red

"""
In url public-subscribe system, use db=0 in redis
"""


def main():
    client = mon.mongo_connection('linode1', 'mongo')
    coll_stockInfo = mon.mongo_collection(client, 'stocks', 'stockInfo_copy')
    redisConnect = red.redis_connection('linode1', 'redis',db=0)
    while True:
        # check if key in redis doesn't exist
        for num in range(1,9):
            key = f'stock_No_{num}'
            if not redisConnect.exists(key):
                # get stock id from mongo and store into redis
                content = coll_stockInfo.find({'crawlerStatus':0},{'_id':1}).limit(1)
                stock_id = content[0]['_id']
                print(f"{key} disapear >>> set {stock_id}")
                coll_stockInfo.update({'_id':stock_id}, {'$set':{'crawlerStatus':1}}) # 表示已經放到redis
                red.redis_set_key_value(redisConnect, key, stock_id)
            print(f"{key} still exist.")
        time.sleep(10)  

if __name__ == '__main__':
    main()