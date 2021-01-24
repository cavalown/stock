from databaseServer import mongoServer as mongo
import requests


def pick_proxy(amount=10):
    prixy_list = []
    client = mongo.mongo_connection('linode1', 'mongo')
    collection = mongo.mongo_collection(client, 'proxy', 'proxyPool_1')
    contents = collection.find({}, {'ip': 1, 'port': 1}).limit(amount)
    for item in contents:
        proxy = item['ip'] + ':' + item['port']
        try:
            validate_proxy(proxy)
            prixy_list.append(proxy)
        except Exception:
            pass
    if len(prixy_list) < 10:
        pick_proxy(10 - len(prixy_list))
    return prixy_list


def validate_proxy(proxy):
    test_ipv4 = 'https://api.ipify.org?format=json'
    try:
        res_test = requests.get(test_ipv4, proxies={'https': proxy, 'http': proxy}, timeout=5)
        print(f"{proxy} OK")
    except Exception:
        print(f"{proxy} Fail")


if __name__ == '__main__':
    proxies = pick_proxy()
    print(proxies)
