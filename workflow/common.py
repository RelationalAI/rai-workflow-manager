import dataclasses
from enum import Enum, EnumMeta
from typing import List, Any

from railib import api

from workflow.constants import AZURE_EXPORT_ACCOUNT, AZURE_EXPORT_CONTAINER, AZURE_EXPORT_DATA_PATH, \
    AZURE_EXPORT_NUM_FILES, AZURE_EXPORT_SAS, AZURE_IMPORT_ACCOUNT, AZURE_IMPORT_CONTAINER, AZURE_IMPORT_DATA_PATH, \
    AZURE_IMPORT_SAS


class MetaEnum(EnumMeta):
    def __contains__(cls, item):
        try:
            cls(item)
        except ValueError:
            return False
        return True


class BaseEnum(Enum, metaclass=MetaEnum):
    pass


class FileFormat(str, BaseEnum):
    CSV = '.csv'
    JSON = '.json'
    JSONL = '.jsonl'
    CSV_GZ = '.csv.gz'
    JSON_GZ = '.csv.gz'
    JSONL_GZ = '.csv.gz'

    @staticmethod
    def is_supported(file_name):
        """
        Check if file has supported format
        :param file_name: File name
        :return: True if a file name ends with supported file format
        """
        for f in FileFormat:
            if file_name.endswith(f):
                return True
        return False


class FileType(str, BaseEnum):
    CSV = 'CSV'
    JSON = 'JSON'
    JSONL = 'JSONL'


class SourceType(Enum):
    LOCAL = 1
    REMOTE = 2

    @staticmethod
    def from_source(src):
        if 'is_local' in src and src['is_local'] == 'Y':
            return SourceType.LOCAL
        elif 'is_remote' in src and src['is_remote'] == 'Y':
            return SourceType.REMOTE
        else:
            raise ValueError("Source is neither local nor remote.")


@dataclasses.dataclass
class Source:
    relation: str
    relative_path: str
    input_format: str
    extensions: List[str]
    is_chunk_partitioned: bool
    is_date_partitioned: bool
    loads_number_of_days: int
    offset_by_number_of_days: int
    snapshot_validity_days: int
    paths: List[str] = dataclasses.field(default_factory=list)

    def to_paths_csv(self) -> str:
        return "\n".join([f"{self.relation},{p}" for p in self.paths])

    def to_chunk_partitioned_paths_csv(self) -> str:
        return "\n".join([f"{self.relation},{path},{self.is_chunk_partitioned}" for path in self.paths])

    def to_formats_csv(self) -> str:
        return f"{self.relation},{self.input_format.upper()}"


@dataclasses.dataclass
class RaiConfig:
    ctx: api.Context
    engine: str
    database: str


@dataclasses.dataclass
class EnvConfig:
    # Azure EXPORT blob
    azure_export_account: str
    azure_export_container: str
    azure_export_data_path: str
    azure_export_num_files: int
    azure_export_sas: str
    # Azure IMPORT blob
    azure_import_account: str
    azure_import_container: str
    azure_import_data_path: str
    azure_import_sas: str

    @staticmethod
    def from_env_vars(env_vars: dict[str, Any]):
        azure_export_account = env_vars[AZURE_EXPORT_ACCOUNT] if AZURE_EXPORT_ACCOUNT in env_vars else ""
        azure_export_container = env_vars[
            AZURE_EXPORT_CONTAINER] if AZURE_EXPORT_CONTAINER in env_vars else ""
        azure_export_data_path = env_vars[
            AZURE_EXPORT_DATA_PATH] if AZURE_EXPORT_DATA_PATH in env_vars else ""
        azure_export_num_files = env_vars[
            AZURE_EXPORT_NUM_FILES] if AZURE_EXPORT_NUM_FILES in env_vars else 1
        azure_export_sas = env_vars[AZURE_EXPORT_SAS] if AZURE_EXPORT_SAS in env_vars else ""
        azure_import_account = env_vars[AZURE_IMPORT_ACCOUNT] if AZURE_IMPORT_ACCOUNT in env_vars else ""
        azure_import_container = env_vars[
            AZURE_IMPORT_CONTAINER] if AZURE_IMPORT_CONTAINER in env_vars else ""
        azure_import_data_path = env_vars[
            AZURE_IMPORT_DATA_PATH] if AZURE_IMPORT_DATA_PATH in env_vars else ""
        azure_import_sas = env_vars[AZURE_IMPORT_SAS] if AZURE_IMPORT_SAS in env_vars else ""
        return EnvConfig(
            azure_export_account=azure_export_account,
            azure_export_container=azure_export_container,
            azure_export_data_path=azure_export_data_path,
            azure_export_num_files=azure_export_num_files,
            azure_export_sas=azure_export_sas,
            azure_import_account=azure_import_account,
            azure_import_container=azure_import_container,
            azure_import_data_path=azure_import_data_path,
            azure_import_sas=azure_import_sas
        )


@dataclasses.dataclass
class Export:
    meta_key: List[str]
    relation: str
    relative_path: str
    file_type: FileType
    snapshot_binding: str
    offset_by_number_of_days: int = 0


@dataclasses.dataclass
class BatchConfig:
    name: str
    content: str
