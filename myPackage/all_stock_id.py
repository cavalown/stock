from myPackage import mongoServer as mon


def all_stock_id():
    client = mon.mongo_connection('linode1', 'mongo')
    collection_stock = mon.mongo_collection(client, 'stocks', "stockInfo")
    contents = list(collection_stock.find({}, {'_id': 1}))
    for item in contents:
        stock_id = item['_id']
        # print(stock_id)
    print("amount of stocks:", len(contents))
    return contents
