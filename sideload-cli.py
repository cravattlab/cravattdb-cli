"""Performs batch import of files into cravattdb through public API."""

from argparse import ArgumentParser
from urllib.parse import urljoin
from getpass import getpass
import utils
import tempfile
import shutil
import requests
import csv
import os

parser = ArgumentParser(description='CLI tool for batch import of files into cravattdb.')
parser.add_argument('url', help='URL of cravattdb instance.')
parser.add_argument('email', help='Email of the user for uploading data.')
parser.add_argument('data_file', help='Path to data file pointing to dataset paths.')
parser.add_argument('headers', help='Headers columns to extract first.', nargs='+')
args = parser.parse_args()


def main():
    """Preprocess data."""
    headers = args.headers
    datasets = []

    with open(args.data_file, 'r') as f:
        for line in csv.reader(f):
            datasets.append({
                'data': dict(zip(headers, line[:len(headers)])),
                'paths': list(filter(None, line[len(headers):]))
            })

    password = getpass('Please enter your CravattDB Password:')
    auth_cookie = utils.login(args.url, args.email, password)

    for item in datasets:
        for folder in item['paths']:
            result = upload(args.url, auth_cookie, folder, item['data'])
            print(result)


def upload(url, auth_cookie, folder, data):
    """Perform upload of dataset."""
    url = urljoin(url, '/api/sideload')

    tmpdir = tempfile.mkdtemp()

    try:
        tmparchive = os.path.join(tmpdir, data['name'])
        zipped = shutil.make_archive(tmparchive, 'zip', folder)

        with open(zipped, 'rb') as f:
            result = requests.put(
                url,
                data,
                files={'file': (f.name, f, 'application/octet-stream')},
                cookies=auth_cookie
            )

            return result
    finally:
        shutil.rmtree(tmpdir)


if __name__ == "__main__":
    main()
