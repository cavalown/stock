# import sys
# sys.path.append(r'/home/cavalown/stock_project/stock')
import yaml


def read_yaml(file_path):
    with open(file_path) as f:
        content = yaml.full_load(f)
    return content

if __name__ == '__main__':
    file_path = '/Users/huangyiling/python_work/python_DB_env/credential/.db.yaml'
    contebts = read_yaml(file_path)
    cn_host = contebts['postgres']['host']
    print(cn_host)
