from myPackage import check_stock_record_if_num as checkIfNum


def compute_records(duration_records):
    doc = dict()
    columns = ['price', 'open', 'high', 'low',
               'close', 'change', 'volume', 'trades']
    # average
    for column in columns[:6]:
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
        if count != 0:
            average = round(sum / count, 2)
            # print('Column:', column, 'Average:', average)
            doc[f'avg{column.title()}'] = average
        else:
            doc[f'avg{column.title()}'] = 'null'
    # sum
    for column in columns[6:]:
        # print('Column:', column)
        sum = 0
        count = 0
        for record in duration_records:
            item = record[column]
            if checkIfNum.check_if_num(item):
                sum += float(item)
                count += 1
        # print('SUM:', sum)
        if count != 0:
            doc[f'sum{column.title()}'] = sum
        else:
            doc[f'sum{column.title()}'] = 'null'
    # print(doc)
    return doc
