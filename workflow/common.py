import dataclasses
from enum import Enum, EnumMeta
from typing import List, Any

from railib import api

from workflow.constants import AZURE_ACCOUNT, AZURE_CONTAINER, AZURE_DATA_PATH, AZURE_SAS, LOCAL_DATA_PATH, CONTAINER, \
    CONTAINER_TYPE, CONTAINER_NAME


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


class ContainerType(str, BaseEnum):
    LOCAL = 'local'
    AZURE = 'azure'

    def __str__(self):
        return self.value

    @staticmethod
    def from_source(src):
        if 'is_local' in src and src['is_local'] == 'Y':
            return ContainerType.LOCAL
        elif 'is_remote' in src and src['is_remote'] == 'Y':
            return ContainerType.AZURE
        else:
            raise ValueError("Source is neither local nor remote.")


@dataclasses.dataclass
class Container:
    name: str
    type: ContainerType
    params: dict[str, Any]


@dataclasses.dataclass
class AzureConfig:
    account: str
    container: str
    data_path: str
    sas: str


@dataclasses.dataclass
class LocalConfig:
    data_path: str


class ConfigExtractor:

    @staticmethod
    def azure_from_env_vars(env_vars: dict[str, Any]):
        return AzureConfig(
            account=env_vars.get(AZURE_ACCOUNT, ""),
            container=env_vars.get(AZURE_CONTAINER, ""),
            data_path=env_vars.get(AZURE_DATA_PATH, ""),
            sas=env_vars.get(AZURE_SAS, "")
        )

    @staticmethod
    def local_from_env_vars(env_vars: dict[str, Any]):
        return LocalConfig(data_path=env_vars.get(LOCAL_DATA_PATH, ""))


@dataclasses.dataclass
class Source:
    container: str
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
        return "\n".join([f"{self.relation},{self.container},{p}" for p in self.paths])

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
    containers: dict[str, Container]

    EXTRACTORS = {
        ContainerType.AZURE: lambda env_vars: ConfigExtractor.azure_from_env_vars(env_vars),
        ContainerType.LOCAL: lambda env_vars: ConfigExtractor.local_from_env_vars(env_vars)
    }

    def container_name_to_type(self) -> dict[str, ContainerType]:
        return {container.name: container.type for container in self.containers.values()}

    def get_container(self, name: str) -> Container:
        return self.containers[name]

    @staticmethod
    def from_env_vars(env_vars: dict[str, Any]):
        containers = {}
        for container in env_vars.get(CONTAINER, []):
            name = container[CONTAINER_NAME]
            containers[name] = Container(name=container[CONTAINER_NAME],
                                         type=ContainerType[container[CONTAINER_TYPE].upper()],
                                         params=container)
        return EnvConfig(containers)


@dataclasses.dataclass
class Export:
    meta_key: List[str]
    relation: str
    relative_path: str
    file_type: FileType
    snapshot_binding: str
    container: str
    offset_by_number_of_days: int = 0


@dataclasses.dataclass
class BatchConfig:
    name: str
    content: str
