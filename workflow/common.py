import dataclasses
from enum import Enum, EnumMeta
from typing import List, Any

from railib import api

from workflow.constants import ACCOUNT_PARAM, CONTAINER_PARAM, DATA_PATH_PARAM, AZURE_SAS, CONTAINER, CONTAINER_TYPE, \
    CONTAINER_NAME, USER_PARAM, PASSWORD_PARAM, SNOWFLAKE_ROLE, SNOWFLAKE_WAREHOUSE, DATABASE_PARAM, SCHEMA_PARAM, \
    FAIL_ON_MULTIPLE_WRITE_TXN_IN_FLIGHT, RAI_SDK_HTTP_RETRIES, RAI_PROFILE, RAI_PROFILE_PATH, \
    SEMANTIC_SEARCH_BASE_URL, RAI_CLOUD_ACCOUNT


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
    SNOWFLAKE = 'snowflake'

    def __str__(self):
        return self.value

    @staticmethod
    def from_source(src):
        try:
            return ContainerType[src["container_type"]]
        except KeyError as ex:
            raise ValueError(f"Container type is not supported: {ex}")


@dataclasses.dataclass
class FileMetadata:
    path: str
    size: int = None
    as_of_date: str = ""


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


@dataclasses.dataclass
class SnowflakeConfig:
    account: str
    user: str
    password: str
    role: str
    warehouse: str
    database: str
    schema: str


class ConfigExtractor:

    @staticmethod
    def azure_from_env_vars(env_vars: dict[str, Any]):
        return AzureConfig(
            account=env_vars.get(ACCOUNT_PARAM, ""),
            container=env_vars.get(CONTAINER_PARAM, ""),
            data_path=env_vars.get(DATA_PATH_PARAM, ""),
            sas=env_vars.get(AZURE_SAS, "")
        )

    @staticmethod
    def snowflake_from_env_vars(env_vars: dict[str, Any]):
        return SnowflakeConfig(
            account=env_vars.get(ACCOUNT_PARAM, ""),
            user=env_vars.get(USER_PARAM, ""),
            password=env_vars.get(PASSWORD_PARAM, ""),
            role=env_vars.get(SNOWFLAKE_ROLE, ""),
            warehouse=env_vars.get(SNOWFLAKE_WAREHOUSE, ""),
            database=env_vars.get(DATABASE_PARAM, ""),
            schema=env_vars.get(SCHEMA_PARAM, "")
        )

    @staticmethod
    def local_from_env_vars(env_vars: dict[str, Any]):
        return LocalConfig(data_path=env_vars.get(DATA_PATH_PARAM, ""))


@dataclasses.dataclass
class Source:
    container: Container
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
        return "\n".join([f"{self.relation},{self.container.name},{p}" for p in self.paths])

    def to_chunk_partitioned_paths_csv(self) -> str:
        return "\n".join([f"{self.relation},{path},{self.is_chunk_partitioned}" for path in self.paths])

    def to_formats_csv(self) -> str:
        return f"{self.relation},{self.input_format.upper()}"

    def to_container_type_csv(self) -> str:
        return f"{self.relation},{self.container.type.name}"

    def is_size_supported(self) -> bool:
        return self.container.type == ContainerType.LOCAL or self.container.type == ContainerType.AZURE


@dataclasses.dataclass
class RaiConfig:
    ctx: api.Context
    engine: str
    database: str


@dataclasses.dataclass
class EnvConfig:
    containers: dict[str, Container]
    fail_on_multiple_write_txn_in_flight: bool = False
    rai_sdk_http_retries: int = 3
    rai_profile: str = "default"
    rai_profile_path: str = "~/.rai/config"
    semantic_search_base_url: str = ""
    rai_cloud_account: str = ""

    __EXTRACTORS = {
        ContainerType.AZURE: lambda env_vars: ConfigExtractor.azure_from_env_vars(env_vars),
        ContainerType.LOCAL: lambda env_vars: ConfigExtractor.local_from_env_vars(env_vars),
        ContainerType.SNOWFLAKE: lambda env_vars: ConfigExtractor.snowflake_from_env_vars(env_vars)
    }

    def get_container(self, name: str) -> Container:
        try:
            return self.containers[name]
        except KeyError:
            raise ValueError(f"Container `{name}` is missed in Environment Config.")

    @staticmethod
    def get_config(container: Container):
        return EnvConfig.__EXTRACTORS[container.type](container.params)

    @staticmethod
    def from_env_vars(env_vars: dict[str, Any]):
        containers = {}
        for container in env_vars.get(CONTAINER, []):
            name = container[CONTAINER_NAME]
            containers[name] = Container(name=container[CONTAINER_NAME],
                                         type=ContainerType[container[CONTAINER_TYPE].upper()],
                                         params=container)
        return EnvConfig(containers, env_vars.get(FAIL_ON_MULTIPLE_WRITE_TXN_IN_FLIGHT, False),
                         env_vars.get(RAI_SDK_HTTP_RETRIES, 3), env_vars.get(RAI_PROFILE, "default"),
                         env_vars.get(RAI_PROFILE_PATH, "~/.rai/config"), env_vars.get(SEMANTIC_SEARCH_BASE_URL, ""),
                         env_vars.get(RAI_CLOUD_ACCOUNT, ""))


@dataclasses.dataclass
class Export:
    meta_key: List[str]
    relation: str
    relative_path: str
    file_type: FileType
    snapshot_binding: str
    container: Container
    offset_by_number_of_days: int = 0


@dataclasses.dataclass
class BatchConfig:
    name: str
    content: str
