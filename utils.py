"""Holds various utility methods needed by CLI scripts."""

from urllib.parse import urljoin
from datetime import datetime
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


def json_serial(obj):
    """JSON serializer for objects not serializable by default json code.

    http://stackoverflow.com/a/22238613/383744
    """
    if isinstance(obj, datetime):
        serial = obj.isoformat()
        return serial
    raise TypeError("Type not serializable")
