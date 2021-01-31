from json import dumps
from httplib2 import Http
from myPackage import read_yaml as ryaml

credential_path = 'credential/google_bot.yaml'

def main(bot_name, message):
    """Hangouts Chat incoming webhook quickstart."""
    content = ryaml.read_yaml(credential_path)
    url = content[bot_name]['webhook']
    bot_message = {'text': message}

    message_headers = {'Content-Type': 'application/json; charset=UTF-8'}

    http_obj = Http()

    response = http_obj.request(
        uri=url,
        method='POST',
        headers=message_headers,
        body=dumps(bot_message),
    )

    print(response)


if __name__ == '__main__':
    main('stock_crawler', 'test again')
