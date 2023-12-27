import logging

import requests
import workflow.rai as rai
from workflow.exception import RestClientException
from workflow.common import RaiConfig


class RestClient:
    def __init__(self, logger: logging.Logger, base_url: str):
        self.logger = logger
        self.base_url = base_url

    def _make_request(self, method, endpoint, data=None, headers=None, files=None):
        url = f"{self.base_url}/{endpoint}"
        try:
            response = requests.request(method, url, data=data, headers=headers, files=files)
            http_error_msg = ""
            status_code = response.status_code
            reason = response.reason
            message = response.text
            if 400 <= response.status_code < 500:
                http_error_msg = f"{status_code} Client Error: {reason} for url: {response.url}. Message: {message}"
            elif 500 <= response.status_code < 600:
                http_error_msg = f"{status_code} Server Error: {reason} for url: {response.url}. Message: {message}"
            if http_error_msg:
                raise RestClientException(method, url, http_error_msg)

            return response
        except Exception as err:
            self.logger.error(f"An error occurred: {err}")
            raise RestClientException(method, url, err)

    def _make_file_request(self, method, endpoint, data=None, headers=None, files=None):
        return self._make_request(method, endpoint, data=data, headers=headers, files=files).content

    def get(self, endpoint, headers=None):
        return self._make_request("GET", endpoint, headers=headers)

    def get_file_content(self, endpoint, headers=None):
        return self._make_file_request("GET", endpoint, headers=headers)

    def post(self, endpoint, data=None, headers=None, files=None):
        return self._make_request("POST", endpoint, data=data, headers=headers, files=files)

    def delete(self, endpoint, headers=None):
        return self._make_request("DELETE", endpoint, headers=headers)

    def put(self, endpoint, data=None, headers=None, files=None):
        return self._make_request("PUT", endpoint, data=data, headers=headers, files=files)


class SemanticSearchRestClient(RestClient):
    def __init__(self, logger: logging.Logger, base_url: str, pod_refix: str):
        super().__init__(logger, base_url)
        self.pod_refix = pod_refix

    def startup(self, rai_config: RaiConfig, account_name: str):
        endpoint = f"semantic-search/v1alpha1/{account_name}/startup?pods=1&disableWarmup=true"
        return self.post(endpoint, headers=self._common_headers(rai_config)).json()

    def shutdown(self, rai_config: RaiConfig, account_name: str):
        endpoint = f"semantic-search/v1alpha1/{account_name}/shutdown"
        self.post(endpoint, headers=self._common_headers(rai_config))

    def get_startup_result(self, rai_config: RaiConfig, account_name: str, operation_id: int):
        endpoint = f"semantic-search/v1alpha1/{account_name}/startupResult?id={operation_id}"
        return self.get(endpoint, headers=self._common_headers(rai_config)).json()

    def init_model_generation(self, rai_config: RaiConfig, account_name: str, metadata, archive):
        endpoint = f"semantic-search/v1alpha1/{account_name}/layers/{rai_config.database}/init-models-generation"
        return self.post(endpoint, headers=self._common_headers(rai_config), files=archive, data=metadata).json()

    def get_async_operation_result(self, rai_config: RaiConfig, account_name: str, operation_id: int):
        endpoint = f"semantic-search/v1alpha1/{account_name}/async-operation-result?id={operation_id}"
        return self.get(endpoint, headers=self._common_headers(rai_config)).json()

    def get_generated_models(self, rai_config: RaiConfig, account_name: str):
        endpoint = f"semantic-search/v1alpha1/{account_name}/layers/{rai_config.database}/generated-models"
        return self.get_file_content(endpoint, headers=self._common_headers(rai_config))

    def create_workflow(self, rai_config: RaiConfig, account_name: str, batch_config: str):
        endpoint = f"semantic-search/v1alpha1/{account_name}/workflows"
        return self.post(endpoint, headers=self._common_headers(rai_config), data=batch_config).json()

    def activate_workflow(self, rai_config: RaiConfig, account_name: str, workflow_id: str):
        endpoint = f"semantic-search/v1alpha1/{account_name}/workflows/{workflow_id}/activate"
        self.put(endpoint, headers=self._common_headers(rai_config))

    def get_enabled_transitions(self, rai_config: RaiConfig, account_name: str, workflow_id: str):
        endpoint = f"semantic-search/v1alpha1/{account_name}/workflows/{workflow_id}/transitions/enabled"
        return self.get(endpoint, headers=self._common_headers(rai_config)).json()

    def fire_transition(self, rai_config: RaiConfig, account_name: str, workflow_id: str, transition: str):
        endpoint = f"semantic-search/v1alpha1/{account_name}/workflows/{workflow_id}/transitions/fire"
        return self.post(endpoint, headers=self._common_headers(rai_config), data=transition).json()

    def get_step_config(self, rai_config: RaiConfig, account_name: str, workflow_id: str, step: str):
        endpoint = f"semantic-search/v1alpha1/{account_name}/workflows/{workflow_id}/steps/{step}"
        return self.get(endpoint, headers=self._common_headers(rai_config)).json()

    def get_step_summary(self, rai_config: RaiConfig, account_name: str, workflow_id: str, step: str):
        endpoint = f"semantic-search/v1alpha1/{account_name}/workflows/{workflow_id}/steps/{step}/summary"
        return self.get(endpoint, headers=self._common_headers(rai_config)).json()

    def get_workflow_summary(self, rai_config: RaiConfig, account_name: str, workflow_id: str):
        endpoint = f"semantic-search/v1alpha1/{account_name}/workflows/{workflow_id}/summary"
        return self.get(endpoint, headers=self._common_headers(rai_config)).json()

    def _common_headers(self, rai_config):
        return {"Authorization": f"Bearer {rai.get_access_token(self.logger, rai_config)}",
                "Pod-Prefix": self.pod_refix}
