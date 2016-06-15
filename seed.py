"""Seeds basic data into CravattDB."""

from argparse import ArgumentParser
from getpass import getpass
from urllib.parse import urljoin
from . import utils
import requests
import json


parser = ArgumentParser(description='CLI tool for seeding data into CravattDB')
parser.add_argument('url', help='URL of cravattdb instance.')
parser.add_argument('email', help='Email of the user for seeding data.')
parser.add_argument('--data', help='JSON file containing data. See seed.json for example', default='seed.json')
args = parser.parse_args()


def main():
    password = getpass('Please enter your CravattDB Password:')
    auth_cookie = utils.login(args.url, args.email, password)

    data = json.loads(args.data)

    for key, value in data.items():
        # this is my singularization algo:
        endpoint = key[:-1]

        for item in value:
            print(seed_item(
                url=args.url,
                endpoint=endpoint,
                item=item,
                auth_cookie=auth_cookie
            ))


def seed_item(url, endpoint, item, auth_cookie):
    url = urljoin(url, 'api', endpoint)
    return requests.put(url, item, cookies=auth_cookie)


if __name__ == "__main__":
    main()
