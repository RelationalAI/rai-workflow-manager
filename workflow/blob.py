import logging
from typing import List
from azure.storage.blob import BlobServiceClient

from workflow.common import FileFormat
from workflow.common import AzureConfig
from workflow.constants import BLOB_PAGE_SIZE


def list_files_in_containers(logger: logging.Logger, config: AzureConfig, path_prefix) -> List[str]:
    azure_account = config.account
    azure_container = config.container
    account_url = f"https://{azure_account}.blob.core.windows.net"

    blob_service_client = BlobServiceClient(account_url=account_url, credential=config.sas)
    container_client = blob_service_client.get_container_client(azure_container)

    # Get a list of blobs in the folder
    logger.debug(f"Path prefix to list blob files: {path_prefix}")
    paths = []
    for page in container_client.list_blobs(name_starts_with=path_prefix, results_per_page=BLOB_PAGE_SIZE).by_page():
        for blob in page:
            blob_name = blob.name
            if FileFormat.is_supported(blob_name):
                paths.append(f"azure://{azure_account}.blob.core.windows.net/{azure_container}/{blob_name}")
            else:
                logger.debug(f"Skip unsupported file from blob: {blob_name}")
    return paths
