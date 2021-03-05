from myPackage import all_stock_id as allStockID
from myPackage import compute_records, duration_records
from myPackage import mongoServer as mon
from myPackage import postgresServer as pos


def month_records():
    client = mon.mongo_connection('linode1', 'mongo')
    # stock id
    for item in allStockID.all_stock_id():
        stock_id = item['_id']
        for year in range(2010, 2022):
            for month in range(1, 13):
                if year == 2021 and month > 2:
                    break
                dur_records = duration_records.get_month_record_mongo(
                    stock_id, year, month)
                if dur_records:
                    docs = compute_records.compute_records(dur_records)
                    # 將計算好的結果存入postgres
                    print(f"{stock_id}: {str(year)+str(month).zfill(2)}")
                    print(docs)
                    break
            break
        break


if __name__ == '__main__':
    month_records()
