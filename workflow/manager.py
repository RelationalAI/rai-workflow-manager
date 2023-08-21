import dataclasses
import copy
import uuid
import logging
from typing import Any
from types import MappingProxyType

from workflow.common import RaiConfig
from workflow import query as q, rai


@dataclasses.dataclass
class EngineMetaInfo:
    engine: str
    size: str
    is_default: bool


class ResourceManager:
    __logger: logging.Logger
    __rai_config: RaiConfig
    __engines: dict[str, EngineMetaInfo]

    def __init__(self, logger: logging.Logger, rai_config: RaiConfig):
        self.__logger = logger
        self.__rai_config = rai_config
        self.__engines = {}

    def get_rai_config(self, size: str = None) -> RaiConfig:
        config = copy.copy(self.__rai_config)
        if size:
            if size in self.__engines:
                config.engine = self.__engines[size].engine
            else:
                self.__logger.debug(f"Can't find `{size}` engine in managed engines. Use default engine")
        return config

    def cleanup_resources(self) -> None:
        """
        Delete RAI DB and engine from RAI Config and managed engines.
        :return:
        """
        self.delete_database()
        for size in self.__engines:
            config = self.get_rai_config(size)
            if rai.engine_exist(self.__logger, config):
                rai.delete_engine(self.__logger, config)

    def create_database(self, delete_db: bool = False, disable_ivm: bool = False, source_db=None) -> None:
        """
        Create RAI database. If `delete_db` is True then manager should delete previous db with the same name if it
         does exist before creation. `disable_ivm` indicate if manager need to disable IVM for a new db.
        :param delete_db:       delete database flag
        :param disable_ivm:     disable ivm flag
        :param source_db:       source database name
        :return:
        """
        if delete_db:
            self.delete_database()
        config = self.get_rai_config()
        rai.create_database(self.__logger, config, source_db)
        if disable_ivm:
            self.__logger.info(f"Disabling IVM for `{config.database}`")
            rai.execute_query(self.__logger, config, q.DISABLE_IVM, readonly=False)

    def delete_database(self) -> None:
        """
        Delete RAI database if it does exist.
        :return:
        """
        config = self.get_rai_config()
        if rai.database_exist(self.__logger, config):
            rai.delete_database(self.__logger, config)

    def add_engine(self, size: str = "XS") -> None:
        """
        Create RAI engine of given size if it doesn't exist and add engine to managed engines.
        The first engine in managed engines should be set as default.
        :param size:    RAI engine size
        :return:
        """
        self.__logger.info(f"Trying to add engine with `{size}` size to manager.")
        config = self.get_rai_config(size)
        if self.__engines:
            if size in self.__engines:
                self.__logger.info(f"`{size}` already managed: `{self.__engines[size]}`. Ignore creation")
            else:
                config.engine = self.__generate_engine_name(size)
                self.__create_engine(config, size)
                self.__engines[size] = EngineMetaInfo(config.engine, size, False)
        else:
            self.__create_engine(config, size)
            self.__engines = {size: EngineMetaInfo(config.engine, size, True)}

    def remove_engine(self, size: str = "XS") -> None:
        """
        Delete RAI engine of given size and removed from managed engines.
        Raise an error when trying to delete the default engine. The default engine can be deleted only by clean up
        resources method.
        :param size:    RAI engine size
        :return:
        """
        self.__logger.info(f"Trying to remove engine with `{size}` size from manager.")
        if size not in self.__engines:
            self.__logger.info(f"`{size}` isn't managed. Ignore deletion")
        else:
            if self.__engines[size].is_default:
                self.__logger.warning(f"Can't remove default `{size}` engine from managed engines")
            else:
                config = self.get_rai_config(size)
                if rai.engine_exist(self.__logger, config):
                    rai.delete_engine(self.__logger, config)
                else:
                    self.__logger.warning(f"Can't find `{config.engine}` engine. Ignore deletion")
                del self.__engines[size]

    def provision_engine(self, size: str) -> None:
        """
        Provision RAI engine of given size and add to managed engines. Delete old engine if it does exist.
        :param size:    RAI engine size
        :return:
        """
        self.__logger.info(f"Trying to provision engine with `{size}` size and add to manager.")
        config = self.get_rai_config(size)
        if self.__engines:
            if size in self.__engines:
                self.__logger.info(
                    f"`{size}` already managed: `{self.__engines[size]}`. Provision engine {config.engine}")
                self.__recreate_engine(config, size)
            else:
                config.engine = self.__generate_engine_name(size)
                self.__logger.info(f"Provision engine `{config.engine}`")
                self.__create_engine(config, size)
                self.__engines[size] = EngineMetaInfo(config.engine, size, False)
        else:
            self.__logger.info(f"Provision engine `{config.engine}` as default.")
            self.__recreate_engine(config, size)
            self.__engines = {size: EngineMetaInfo(config.engine, size, True)}

    def __recreate_engine(self, config, size: str) -> None:
        """
        Delete old engine if it exists and create new one.
        :param config:  RAI config
        :param size:    RAI engine size
        :return:
        """
        if rai.engine_exist(self.__logger, config):
            rai.delete_engine(self.__logger, config)
        rai.create_engine(self.__logger, config, size)

    def __create_engine(self, config, size: str) -> None:
        """
        Create RAI engine if it doesn't exist.
        :param size:    RAI engine size
        :return:
        """
        if not rai.engine_exist(self.__logger, config):
            rai.create_engine(self.__logger, config, size)

    @staticmethod
    def __generate_engine_name(size: str) -> str:
        """
        Generate RAI engine name for workflow.
        :param size:    RAI engine size
        :return:
        """
        return f"wf-manager-{size}-{uuid.uuid4()}"

    @staticmethod
    def init(logger: logging.Logger, engine, database, env_vars: dict[str, Any] = MappingProxyType({})):
        logger = logger.getChild("workflow_resource_manager")
        rai_config = rai.get_config(engine, database, env_vars)
        return ResourceManager(logger, rai_config)
