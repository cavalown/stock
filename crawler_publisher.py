from myPackage import mongoServer as mon
from myPackage import redisServer as red

"""
In url public-subscribe system, use db=0 in redis
"""


def main():
    client = mon.mongo_connection('linode1', 'mongo')
    coll_stockInfo = mon.mongo_collection(client, 'stocks', 'stockInfo')
    redisConnect = red.redis_connection('linode1', 'redis',db=0)
    while True:
        # check if key in redis doesn't exist
        for num in range(1,13):
            key = f'stock_NO_{num}'
            if not redisConnect.exists(key):
                print(f"{key} disapear")
                # get stock id from mongo and store into redis
                content = coll_stockInfo.find({'yearStatus':0},{'_id':1}).limit(1)
                stock_id = content[0]['_id']
                print(stock_id)
                # redis.redis_set_key_value(redisConnect, key, stock_id)
                # mongo.update_one({'_id':stock_id}, {'$set':{'yearStatus':1}})
                break
            print(f"{key} still exist.")
            break
    

if __name__ == '__main__':
    main()