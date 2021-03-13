from myPackage import all_stock_id as allStockID
from myPackage import compute_records, duration_records
from myPackage import mongoServer as mon
from myPackage import postgresServer as pos


def season_records():
    # set moongosdb and postgres connection
    client = mon.mongo_connection('linode1', 'mongo')
    conn_pos = pos.postgres_connection('linode1', 'postgres', 'stock')
    cursor_pos = pos.make_cursor(connection=conn_pos)
    # stock id
    for item in allStockID.all_stock_id():
        stock_id = item['_id']
        # create table if not exists with table name like month+stock_id
        sql = f"""
        create table if not exists season{stock_id} (
        ID varchar(10) primary key,
        duration char(6),
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
        for year in range(2010, 2021):
            for season in range(1, 5):
                dur_records = duration_records.get_season_record_mongo(
                    stock_id, year, season)
                if dur_records:
                    # print('dur', dur_records)
                    docs = compute_records.compute_records(dur_records)
                    # 將計算好的結果存入postgres
                    print(f"{stock_id}: {year}, season: {season}")
                    print(docs)
                    query = f"""
                    INSERT INTO season{stock_id}
                    (ID, duration, sum_volumn, sum_trade, avg_price, avg_open, avg_high, avg_low, avg_close, avg_change)
                    VALUES
                    ('{stock_id+str(year)+str(season).zfill(2)}', '{str(year)+str(season).zfill(2)}', {docs['sumVolume']}, {docs['sumTrades']}, 
                    {docs['avgPrice']}, {docs['avgOpen']}, {docs['avgHigh']},{docs['avgLow']}, {docs['avgClose']}, {docs['avgChange']})
                    """
                    if not conn_pos:
                        conn_pos = pos.postgres_connection(
                            'linode1', 'postgres', 'stock')
                        cursor_pos = pos.make_cursor(connection=conn_pos)
                    pos.insertTable(connection=conn_pos,
                                    cursor=cursor_pos, query=query, exceptionfile='oldseason')
    pos.close_connection(connection=conn_pos)
    mon.close_connection(client=client)


if __name__ == '__main__':
    season_records()
