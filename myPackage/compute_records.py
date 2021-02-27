from myPackage import check_stock_record_if_num as checkIfNum

def compute_records(duration_records):
    doc = dict()
    columns = ['price', 'open', 'high', 'low', 'close', 'volume', 'trades']
    # average
    for column in columns[:5]:
        # print('Column:', column)
        sum = 0
        count = 0
        for record in duration_records:
            # print(record)
            item = record[column]
            # print(item)
            if checkIfNum.check_if_num(item):
                sum += float(item)
                count += 1
        # print('SUM:', sum)
        # print('COUNT:', count)
        average = sum / count
        # print('Column:', column, 'Average:', average)
        doc[f'avg{column.title()}'] = average
    # sum
    for column in columns[5:]:
        # print('Column:', column)
        sum = 0
        for record in duration_records:
            item = record[column]
            if checkIfNum.check_if_num(item):
                sum += float(item)
        # print('SUM:', sum)
        doc[f'sum{column.title()}'] = sum
    # print(doc)
    return doc
