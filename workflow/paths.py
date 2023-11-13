import glob
import logging
import os.path
import pathlib
from typing import List

from workflow import blob, constants
from workflow.common import EnvConfig, AzureConfig, LocalConfig, SnowflakeConfig, Container, ContainerType, FileMetadata


class PathsBuilder:

    def build(self, logger: logging.Logger, days: List[str], relative_path: str, input_format,
              is_date_partitioned: bool) -> List[FileMetadata]:
        paths = self._build(logger, days, relative_path, input_format, is_date_partitioned)
        if not paths:
            logger.warning(f"""PathsBuilder didn't find any file for specified parameters:
            days=`{days}`, relative_path=`{relative_path}`, input_format=`{input_format}`, 
            is_date_partitioned=`{is_date_partitioned}`""")
        return paths

    def _build(self, logger: logging.Logger, days: List[str], relative_path: str, input_format,
               is_date_partitioned: bool) -> List[FileMetadata]:
        raise NotImplementedError("This class is abstract")


class LocalPathsBuilder(PathsBuilder):
    config: LocalConfig

    def __init__(self, config):
        self.config = config

    def _build(self, logger: logging.Logger, days: List[str], relative_path, extensions: List[str],
               is_date_partitioned: bool) -> List[FileMetadata]:
        paths = []
        files_path = f"{self.config.data_path}/{relative_path}"
        if is_date_partitioned:
            for day in days:
                folder_path = f"{files_path}/{constants.DATE_PREFIX}{day}"
                day_paths = [FileMetadata(os.path.abspath(path), os.path.getsize(path), day) for path in
                             self._get_folder_paths(folder_path, extensions)]
                paths.extend(day_paths)
        else:
            paths = [FileMetadata(os.path.abspath(path), os.path.getsize(path)) for path in
                     self._get_folder_paths(files_path, extensions)]
        return paths

    @staticmethod
    def _get_folder_paths(folder_path: str, extensions: List[str]):
        paths = []
        if pathlib.Path(folder_path).is_dir():
            for ext in extensions:
                paths.extend(glob.glob(f"{folder_path}/*.{ext}"))
        return paths


class AzurePathsBuilder(PathsBuilder):
    config: AzureConfig

    def __init__(self, config: AzureConfig):
        self.config = config

    def _build(self, logger: logging.Logger, days: List[str], relative_path, extensions: List[str],
               is_date_partitioned: bool) -> List[FileMetadata]:
        import_data_path = self.config.data_path
        files_path = f"{import_data_path}/{relative_path}"

        logger.info(f"Loading data from blob import path: '{import_data_path}'")

        paths = []
        if is_date_partitioned:
            for day in days:
                logger.debug(f"Day from range: {day}")
                day_paths = blob.list_files_in_containers(logger, self.config,
                                                          f"{files_path}/{constants.DATE_PREFIX}{day}")
                for path in day_paths:
                    path.as_of_date = day
                paths += day_paths
        else:
            paths = blob.list_files_in_containers(logger, self.config, files_path)
        return paths


class SnowflakePathsBuilder(PathsBuilder):
    config: SnowflakeConfig

    def __init__(self, config: SnowflakeConfig):
        self.config = config

    def _build(self, logger: logging.Logger, days: List[str], relative_path, extensions: List[str],
               is_date_partitioned: bool) -> List[FileMetadata]:
        return [FileMetadata(path=f"{self.config.database}.{self.config.schema}.{relative_path}")]


class PathsBuilderFactory:
    __CONTAINER_TYPE_TO_BUILDER = {
        ContainerType.LOCAL: lambda container: LocalPathsBuilder(EnvConfig.get_config(container)),
        ContainerType.AZURE: lambda container: AzurePathsBuilder(EnvConfig.get_config(container)),
        ContainerType.SNOWFLAKE: lambda container: SnowflakePathsBuilder(EnvConfig.get_config(container)),
    }

    @staticmethod
    def get_path_builder(container: Container) -> PathsBuilder:
        return PathsBuilderFactory.__CONTAINER_TYPE_TO_BUILDER[container.type](container)
