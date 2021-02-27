def check_if_num(item):
    item = str(item)
    # float(0.00) if change_ori == 'X0.00' else float(change_ori)
    if item in ['X0.00', 'x0.00']:
        return False
    elif len(item.split('.')) > 1:
        left = item.split('.')[0]
        right = item.split('.')[1]
        if left.isdigit() and right.isdigit():
            return True
        return False
    else:
        return False
