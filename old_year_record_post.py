from myPackage import all_stock_id as allStockID
from myPackage import compute_records, duration_records
from myPackage import mongoServer as mon
from myPackage import postgresServer as pos


def year_records():
    # set moongosdb and postgres connection
    client = mon.mongo_connection('linode1', 'mongo')
    conn_pos = pos.postgres_connection('linode1', 'postgres', 'stock')
    cursor_pos = pos.make_cursor(connection=conn_pos)
    # stock id
    for item in allStockID.all_stock_id():
        stock_id = item['_id']
        for item in allStockID.all_stock_id():
            stock_id = item['_id']
        # create table if not exists with table name like month+stock_id
        sql = f"""
        create table if not exists year{stock_id} (
        ID varchar(8) primary key,
        duration char(4),
        sum_volumn decimal,
        avg_price decimal,
        avg_open decimal,
        avg_high decimal,
        avg_low decimal,
        avg_close decimal,
        avg_change decimal,
        sum_trade decimal
        );
        """
        pos.createTable(connection=conn_pos, cursor=cursor_pos, sql=sql)
        # iterately insert data into table
        for year in range(2010, 2021):
            dur_records = duration_records.get_year_record_mongo(stock_id, year)
            if dur_records:
                docs = compute_records.compute_records(dur_records)
                # 將計算好的結果存入postgres
                print(f"{stock_id}: {year}")
                print(docs)
                query = f"""
                INSERT INTO year{stock_id}
                (ID, duration, sum_volumn, sum_trade, avg_price,avg_open, avg_high, avg_low, avg_close, avg_change)
                VALUES
                ('{stock_id+str(year)}', '{str(year)}', {docs['sumVolume']}, {docs['sumTrades']},
                {docs['avgPrice']}, {docs['avgOpen']}, {docs['avgHigh']},{docs['avgLow']}, {docs['avgClose']}, {docs['avgChange']})
                """
                if not conn_pos:
                    conn_pos = pos.postgres_connection(
                        'linode1', 'postgres', 'stock')
                    cursor_pos = pos.make_cursor(connection=conn_pos)
                pos.insertTable(connection=conn_pos,
                                cursor=cursor_pos, query=query, exceptionfile='oldyear')
    pos.close_connection(connection=conn_pos)
    mon.close_connection(client=client)


if __name__ == '__main__':
    year_records()
