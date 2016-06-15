"""Holds various utility methods needed by CLI scripts."""

from urllib.parse import urljoin
import requests


def login(url, email, password):
    """Obtain authentication cookie from CravattDB."""
    csrf_req = requests.get(urljoin(url, '/login_csrf'))
    csrf_token = csrf_req.json()['csrf_token']

    login_req = requests.post(
        urljoin(url, '/login'),
        {
            'email': email,
            'password': password,
            'csrf_token': csrf_token
        },
        cookies=csrf_req.cookies,
        allow_redirects=False
    )

    return login_req.cookies
