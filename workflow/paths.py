import glob
import logging
import os.path
import pathlib
from typing import List
from datetime import datetime

from workflow.common import EnvConfig
from workflow import blob, constants


class PathsBuilder:

    def build(self, logger: logging.Logger, date_range: List[datetime], relative_path: str, input_format,
              is_date_partitioned: bool) -> List[str]:
        paths = self._build(logger, date_range, relative_path, input_format, is_date_partitioned)
        if not paths:
            raise AssertionError(f"""PathsBuilder didn't find any file for specified parameters:
            date_range=`{date_range}`, relative_path=`{relative_path}`, 
            input_format=`{input_format}`, is_date_partitioned=`{is_date_partitioned}`""")
        return paths

    def _build(self, logger: logging.Logger, date_range: List[datetime], relative_path: str, input_format,
               is_date_partitioned: bool) -> List[str]:
        raise NotImplementedError("This class is abstract")


class LocalPathsBuilder(PathsBuilder):
    local_data_dir: str

    def __init__(self, local_data_dir):
        self.local_data_dir = local_data_dir

    def _build(self, logger: logging.Logger, date_range: List[datetime], relative_path, extensions: List[str],
               is_date_partitioned: bool) -> List[str]:
        paths = []
        files_path = f"{self.local_data_dir}/{relative_path}"
        if is_date_partitioned:
            for day in date_range:
                d = day.strftime(constants.DATE_FORMAT)
                folder_path = f"{files_path}/{constants.DATE_PREFIX}{d}"
                paths.extend(self._get_folder_paths(folder_path, extensions))
        else:
            paths = self._get_folder_paths(files_path, extensions)

        paths = [os.path.abspath(path) for path in paths]
        return paths

    @staticmethod
    def _get_folder_paths(folder_path: str, extensions: List[str]):
        paths = []
        if pathlib.Path(folder_path).is_dir():
            for ext in extensions:
                paths.extend(glob.glob(f"{folder_path}/*.{ext}"))
        return paths


class RemotePathsBuilder(PathsBuilder):
    env_config: EnvConfig

    def __init__(self, env_config: EnvConfig):
        self.env_config = env_config

    def _build(self, logger: logging.Logger, date_range: List[datetime], relative_path, extensions: List[str],
               is_date_partitioned: bool) -> List[str]:
        import_data_path = self.env_config.azure_import_data_path
        files_path = f"{import_data_path}/{relative_path}"

        logger.info(f"Loading data from blob import path: '{import_data_path}'")

        paths = []
        if is_date_partitioned:
            for day in date_range:
                d = day.strftime(constants.DATE_FORMAT)
                logger.debug(f"Day from range: {d}")
                paths += blob.list_files_in_containers(logger, self.env_config,
                                                       f"{files_path}/{constants.DATE_PREFIX}{d}")
        else:
            paths = blob.list_files_in_containers(logger, self.env_config, files_path)
        return paths
