from myPackage import mongoServer as mon


def stockInfo():
    client = mon.mongo_connection('linode1', 'mongo')
    coll_stockIndustry = mon.mongo_collection(
        client, 'stocks', 'stockIndustry')
    coll_stockInfo = mon.mongo_collection(client, 'stocks', 'stockInfo')
    for item in mon.find_all_mongo(coll_stockIndustry):
        # print(item)
        ids = item['stocks_list']
        for stock_id in ids:
            doc = {
                '_id': stock_id,
                'industry': item['_id'],
                'name': 'name',
                'abbreviation': item['stocks'][stock_id],
                'dailyStatus': 0,
                'monthStatus': 0,
                'yearStatus': 0}
            print(doc)
            mon.insert_document(coll_stockInfo, doc)


if __name__ == '__main__':
    stockInfo()
