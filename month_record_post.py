from myPackage import mongoServer as mon
from myPackage import postgresServer as pos 
import record_to_postgres as rtp

def get_stock_id():
    client = mon.mongo_connection('linode1', 'mongo')
    collection = mon.mongo_collection(client, 'stocks', 'stockInfo')
    stockInfo_contents =  collection.find({},{'_id':1})
    return stockInfo_contents

def main():
    stock_infos = get_stock_id()
    for stock_info in stock_infos:
        stock_id = stock_info['_id']
        rtp.record_computed_month_to_post(stock_id)


if __name__ == '__main__':
    main()
    