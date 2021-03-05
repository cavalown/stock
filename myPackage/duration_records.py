import re
from myPackage import mongoServer as mon
from myPackage import postgresServer as pos


def get_month_record_mongo(stock_id, year, month):
    client = mon.mongo_connection('linode1', 'mongo')
    collection = mon.mongo_collection(client, 'stocks', f"stock{stock_id}")
    # 檢查當是否有存資料
    if collection.find({"trade_date": {"$regex": f"{str(year)+str(month)}"}}, {"trade_date": 1}).count() != 0:
        contents = list(collection.find({'_id': re.compile(f"{stock_id}{year}{month}")}))
        return contents
    else:
        return


def get_3_month_records(collection, stock_id, year, month_list):
    contents = list(collection.find(
        {'$or': [{'_id': re.compile(f"{stock_id}{year}{month_list[0]}")},
                 {'_id': re.compile(f"{stock_id}{year}{month_list[1]}")},
                 {'_id': re.compile(f"{stock_id}{year}{month_list[2]}")}]
         }))
    return contents


def get_season_record_mongo(stock_id, year, season):
    client = mon.mongo_connection('linode1', 'mongo')
    collection = mon.mongo_collection(client, 'stocks', f"stock{stock_id}")
    print('count_documents:', collection.count_documents({}))
    if season == 1:
        contents = get_3_month_records(
            collection, stock_id, year, ['01', '02', '03'])
        return contents
    elif season == 2:
        contents = get_3_month_records(
            collection, stock_id, year, ['04', '05', '06'])
        return contents
    elif season == 3:
        contents = get_3_month_records(
            collection, stock_id, year, ['07', '08', '09'])
        return contents
    elif season == 4:
        contents = get_3_month_records(
            collection, stock_id, year, ['10', '11', '12'])
        return contents
    else:
        return


def get_year_record_mongo(stock_id, year):
    client = mon.mongo_connection('linode1', 'mongo')
    collection = mon.mongo_collection(client, 'stocks', f"stock{stock_id}")
    contents = list(collection.find({'_id': re.compile(f"{stock_id}{year}")}))
    return contents











if __name__ == '__main__':
    # contents = get_month_record_mongo('1110', '2010', '01')
    # print(contents)
    # compute_month(contents)
    # print('='*10)
    # contents_year = get_year_record_mongo('stock1101', '2010')
    # compute_year(contents_year)
    contents_season = get_season_record_mongo('1110', '2010', '1')
    print(contents_season)
