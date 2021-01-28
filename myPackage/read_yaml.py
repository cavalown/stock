import yaml


def read_yaml(file_path):
    with open(file_path) as f:
        content = yaml.full_load(f)
    return content


if __name__ == '__main__':
    file_path = '/Users/huangyiling/credential/db.yaml'
    contebts = read_yaml(file_path)
    cn_host = contebts['linode1']['postgres']['host']
    print(cn_host)
