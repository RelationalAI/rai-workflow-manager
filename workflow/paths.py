import dataclasses
import glob
import logging
import os.path
import pathlib
from typing import List

from workflow import blob, constants
from workflow.common import EnvConfig, AzureConfig, LocalConfig, Container, ContainerType


@dataclasses.dataclass
class FilePath:
    path: str
    as_of_date: str = None


class PathsBuilder:

    def build(self, logger: logging.Logger, days: List[str], relative_path: str, input_format,
              is_date_partitioned: bool) -> List[FilePath]:
        paths = self._build(logger, days, relative_path, input_format, is_date_partitioned)
        if not paths:
            raise AssertionError(f"""PathsBuilder didn't find any file for specified parameters:
            days=`{days}`, relative_path=`{relative_path}`, input_format=`{input_format}`, 
            is_date_partitioned=`{is_date_partitioned}`""")
        return paths

    def _build(self, logger: logging.Logger, days: List[str], relative_path: str, input_format,
               is_date_partitioned: bool) -> List[FilePath]:
        raise NotImplementedError("This class is abstract")


class LocalPathsBuilder(PathsBuilder):
    config: LocalConfig

    def __init__(self, config):
        self.config = config

    def _build(self, logger: logging.Logger, days: List[str], relative_path, extensions: List[str],
               is_date_partitioned: bool) -> List[FilePath]:
        paths = []
        files_path = f"{self.config.data_path}/{relative_path}"
        if is_date_partitioned:
            for day in days:
                folder_path = f"{files_path}/{constants.DATE_PREFIX}{day}"
                day_paths = [FilePath(path=os.path.abspath(path), as_of_date=day) for path in
                             self._get_folder_paths(folder_path, extensions)]
                paths.extend(day_paths)
        else:
            paths = [FilePath(path=os.path.abspath(path)) for path in self._get_folder_paths(files_path, extensions)]
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
               is_date_partitioned: bool) -> List[FilePath]:
        import_data_path = self.config.data_path
        files_path = f"{import_data_path}/{relative_path}"

        logger.info(f"Loading data from blob import path: '{import_data_path}'")

        paths = []
        if is_date_partitioned:
            for day in days:
                logger.debug(f"Day from range: {day}")
                day_paths = [FilePath(path=path, as_of_date=day) for path in
                             blob.list_files_in_containers(logger, self.config,
                                                           f"{files_path}/{constants.DATE_PREFIX}{day}")]
                paths += day_paths
        else:
            paths = [FilePath(path=path) for path in blob.list_files_in_containers(logger, self.config, files_path)]
        return paths


class PathsBuilderFactory:

    __CONTAINER_TYPE_TO_BUILDER = {
        ContainerType.LOCAL: lambda container: LocalPathsBuilder(EnvConfig.EXTRACTORS[container.type](container.params)),
        ContainerType.AZURE: lambda container: AzurePathsBuilder(EnvConfig.EXTRACTORS[container.type](container.params))
    }

    @staticmethod
    def get_path_builder(container: Container) -> PathsBuilder:
        return PathsBuilderFactory.__CONTAINER_TYPE_TO_BUILDER[container.type](container)

