from myPackage import all_stock_id as allStockID
from myPackage import compute_records, duration_records
from myPackage import mongoServer as mon
from myPackage import postgresServer as pos


def season_records():
    client = mon.mongo_connection('linode1', 'mongo')
    # stock id
    for item in allStockID.all_stock_id():
        stock_id = item['_id']
        for year in range(2010, 2021):
            for season in range(1, 5):
                if year == 2021:
                    break
                dur_records = duration_records.get_season_record_mongo(
                    stock_id, year, season)
                if dur_records:
                    # print('dur', dur_records)
                    docs = compute_records.compute_records(dur_records)
                    # 將計算好的結果存入postgres
                    print(f"{stock_id}: {year}, season: {season}")
                    print(docs)
            break
        break


if __name__ == '__main__':
    season_records()
