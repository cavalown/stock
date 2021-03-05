from myPackage import all_stock_id as allStockID
from myPackage import compute_records, duration_records
from myPackage import mongoServer as mon
from myPackage import postgresServer as pos


def year_records():
    client = mon.mongo_connection('linode1', 'mongo')
    # stock id
    for item in allStockID.all_stock_id():
        stock_id = item['_id']
        for year in range(2010, 2021):
            if year == 2021:
                break
            dur_records = duration_records.get_year_record_mongo(stock_id, year)
            if dur_records:
                docs = compute_records.compute_records(dur_records)
                # 將計算好的結果存入postgres
                print(f"{stock_id}: {year}")
                print(docs)
                
        break


if __name__ == '__main__':
    year_records()
