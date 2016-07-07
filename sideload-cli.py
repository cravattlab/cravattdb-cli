"""
Performs batch import of files into cravattdb through public API.

There is much room for optimization here but code clarity > performance.
"""

from argparse import ArgumentParser
from urllib.parse import urljoin
from getpass import getpass
from collections import defaultdict
from copy import deepcopy
import utils
import tempfile
import shutil
import requests
import pathlib
import openpyxl

parser = ArgumentParser(description='CLI tool for batch import of files into cravattdb.')
parser.add_argument('url', help='URL of cravattdb instance.')
parser.add_argument('email', help='Email of the user for uploading data.')
parser.add_argument('data_file', help='Path to data file pointing to dataset paths. Use sideload-template.xlsx.')
args = parser.parse_args()


# holds default name for dta folder as specified by ratio numerator
DEFAULT_DTA_PATH = {
    'H': 'dta_HL',
    'L': 'dta'
}

# items that should have names replaced with numeric ids
HEADERS_TO_REPLACE = [
    'organism',
    'experiment_type',
    'instrument',
    'treatment_type',
    'proteomic_fraction',
    'sample_type',
    'cell_type',
    'probe',
    'inhibitor'
]


def main():
    """Preprocess data."""
    datasets = []

    ws = openpyxl.load_workbook(args.data_file, read_only=True, data_only=True).active
    rows = [row for row in ws.rows]
    headers = [row.value for row in rows[1]]
    # all headers up to 'path' represent data, the rest are paths
    last_data_index = headers.index('path')
    headers = headers[:last_data_index]

    # first two rows are headers
    for line in rows[2:]:
        line = [item.value for item in line]
        datasets.append({
            'data': dict(zip(headers, line[:len(headers)])),
            'paths': list(filter(None, line[len(headers):]))
        })

    data_columns = [[item.value for item in column[2:]] for column in ws.columns[:last_data_index]]
    datasets = replace_names_with_ids(headers, datasets, data_columns)
    flatten(datasets)

    password = getpass('Please enter your CravattDB Password:')
    auth_cookie = utils.login(args.url, args.email, password)

    for item in datasets:
        for folder in item['paths']:
            result = upload(args.url, auth_cookie, folder, item['data'])
            print(result)


def upload(url, auth_cookie, folder, data):
    """Perform upload of dataset."""
    url = urljoin(url, '/api/sideload')

    dataset_name = data['name']
    folder_path = pathlib.Path(folder)
    dta_name = folder_path.stem

    # create temp directory into which to copy whitelisted and corrected files
    # context manager ensures that all of this will be deleted
    with tempfile.TemporaryDirectory() as tmpdir:
        # separate folder within the temp folder which will contain data
        tmp_path = pathlib.Path(tmpdir, dataset_name)

        # take first parent of folder since the path we're provided should
        # point to the dta folder
        cleaned_path = clean_copy(folder_path.parents[0], tmp_path, dta_name, data)

        zipped = shutil.make_archive(
            str(pathlib.Path(tmpdir, dataset_name)),
            'zip',
            str(cleaned_path)
        )

        with open(zipped, 'rb') as f:
            result = requests.put(
                url,
                data,
                files={'file': (f.name, f, 'application/octet-stream')},
                cookies=auth_cookie
            )

            return result


def clean_copy(folder_path, temp_dest, dta_name, data):
    """Clean cimage data directory of extraneous files and folders.

    Additionally renames folders/files and corrects links in .txt and .html
    files for consistency.
    """
    whitelist = _generate_whitelist(dta_name)

    dest_path = pathlib.Path(shutil.copytree(
        str(folder_path),
        str(temp_dest),
        ignore=_whitelist_toplevel(folder_path, whitelist)
    ))

    rename_folders(
        dest_path,
        dta_name,
        DEFAULT_DTA_PATH[data['ratio_numerator']],
        whitelist
    )

    return dest_path


def rename_folders(folder_path, current_dta_name, correct_dta_name, whitelist):
    """Rename folders and files if using a non-standard dta folder."""
    # if we're already using the correct dta folder name then we're done here!
    if current_dta_name == correct_dta_name:
        return
    else:
        corrected_whitelist = _generate_whitelist(correct_dta_name)

        for index, item in enumerate(whitelist):
            folder_path.joinpath(item).rename(
                folder_path.joinpath(corrected_whitelist[index])
            )

        fix_broken_links(folder_path, current_dta_name, correct_dta_name)


def fix_broken_links(folder_path, current_dta_name, correct_dta_name):
    """Fix broken links after renaming dta folders."""
    # this assumes files have been renamed prior to calling this function
    files_to_correct = (
        'combined_{}.txt'.format(correct_dta_name),
        'combined_{}.html'.format(correct_dta_name)
    )

    for file_to_correct in files_to_correct:
        file_path = folder_path.joinpath(file_to_correct)
        raw = None

        with file_path.open('r') as f:
            raw = f.read()

        raw = raw.replace(current_dta_name, correct_dta_name)

        with file_path.open('w') as f:
            f.write(raw)


def replace_names_with_ids(headers, datasets, columns):
    """Replace string names with numeric ids which the API can consume directly."""
    for index, header in enumerate(headers):
        if header in HEADERS_TO_REPLACE:
            name_to_id_map = {item: get_item_id(header, item) for item in set(columns[index])}
            for j, item in enumerate(datasets):
                datasets[j]['data'][header] = name_to_id_map[item['data'][header]]

    return datasets


def get_item_id(endpoint, name):
    """Get numerical id for a given item on a specific endpoint. Creates new item if does not exist.

    Performs GET request to /api/endpoint, searches for item in result list. If it does not exist,
    performs PUT request to /api/endpoint and returns id of newly created item.

    Arguments:
        endpoint {string} -- API endpoint. /api/ENDPOINT
        item {string} -- Name of item.
    """
    url = urljoin(args.url, 'api/{}'.format(endpoint))

    result = requests.get(url).json()

    # unwrap pluralized result
    contents = next(iter(result.values()))
    # find first instance of item which has matching name
    item = next((x for x in contents if x['name'] == name), None)

    if item:
        return item['id']
    else:
        return requests.put(url, {'name': name}).json()['id']


def flatten(datasets):
    """Package multiple headers into objects if they contain a . in their name."""
    for item in datasets:
        data = item['data']
        temp_data = {}

        for key in iter(data.keys()):
            if '.' in key:
                layers = key.split('.')
                root = nested_dict()
                # first element is first node
                node = root[layers[0]]

                # loop over everything but the first and last elements
                for layer in layers[1:-1]:
                    # middle elements are intermediate nodes
                    node = node[layer]

                # last element is the value that we've been nesting all this way to include
                node[layers[-1]] = data[key]
                temp_data = dict_merge(temp_data, dictify_nested(root))
            else:
                temp_data[key] = data[key]

        item['data'] = temp_data


def nested_dict():
    """Create arbitratily nested dictionaries."""
    return defaultdict(nested_dict)


def dictify_nested(d):
    """Convert nested defaultdict to dict."""
    for k, v in d.items():
        if isinstance(v, dict):
            d[k] = dictify_nested(v)

    return dict(d)


def dict_merge(a, b):
    """Recursively merge two dicts.

    Lifted from: https://www.xormedia.com/recursively-merge-dictionaries-in-python/
    """
    if not isinstance(b, dict):
        return b
    result = deepcopy(a)
    for k, v in b.items():
        if k in result and isinstance(result[k], dict):
                result[k] = dict_merge(result[k], v)
        else:
            result[k] = deepcopy(v)
    return result


def _generate_whitelist(dta_name):
    """Generate whitelist of files for a particular dta folder name."""
    return sorted(set([
        dta_name,
        'combined_{}.html'.format(dta_name),
        'combined_{}.png'.format(dta_name),
        'combined_{}.txt'.format(dta_name),
        'combined_{}.vennDiagram.png'.format(dta_name)
    ]))


def _whitelist_toplevel(folder_path, whitelist):
    toplevel = str(folder_path)

    def _ignore(folder, contents):
        if folder == toplevel:
            return [f for f in contents if f not in whitelist]
        else:
            return []

    return _ignore

if __name__ == "__main__":
    main()
