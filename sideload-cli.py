"""Performs batch import of files into cravattdb through public API."""

from argparse import ArgumentParser
from urllib.parse import urljoin
from getpass import getpass
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


def main():
    """Preprocess data."""
    datasets = []

    ws = openpyxl.load_workbook(args.data_file, read_only=True, data_only=True).active
    rows = [row for row in ws.rows]
    headers = [row.value for row in rows[1]]
    # all headers up to 'path' represent data, the rest are paths
    headers = headers[:headers.index('path')]

    # first two rows are headers
    for line in rows[2:]:
        line = [item.value for item in line]
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

    dataset_name = data['name']
    folder_path = pathlib.Path(folder)
    dta_name = folder_path.stem

    # create temp directory into which to copy whitelisted and corrected files
    # context manager ensures that all of this will be deleted
    with tempfile.TemporaryDirectory as tmpdir:
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
