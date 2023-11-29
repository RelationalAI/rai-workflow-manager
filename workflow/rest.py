import logging

import requests
from exception import RestClientException


class RestClient:
    def __init__(self, logger: logging.Logger, base_url: str):
        self.logger = logger
        self.base_url = base_url

    def _make_request(self, method, endpoint, data=None, headers=None, files=None):
        url = f"{self.base_url}/{endpoint}"
        try:
            response = requests.request(method, url, data=data, headers=headers, files=files)
            response.raise_for_status()  # Raise an HTTPError for bad responses (4xx or 5xx)
            return response.json()
        except requests.exceptions.HTTPError as http_err:
            self.logger.error(f"HTTP error occurred: {http_err}")
            raise RestClientException(method, url, http_err)
        except Exception as err:
            self.logger.error(f"An error occurred: {err}")
            raise RestClientException(method, url, err)

    def get(self, endpoint, headers=None):
        return self._make_request("GET", endpoint, headers=headers)

    def post(self, endpoint, data=None, headers=None, files=None):
        return self._make_request("POST", endpoint, data=data, headers=headers, files=files)

    def delete(self, endpoint, headers=None):
        return self._make_request("DELETE", endpoint, headers=headers)

    def put(self, endpoint, data=None, headers=None, files=None):
        return self._make_request("PUT", endpoint, data=data, headers=headers, files=files)

