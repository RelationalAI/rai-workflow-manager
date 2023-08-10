import logging
from typing import Any
from types import MappingProxyType

from workflow.common import RaiConfig
from workflow import query as q, rai


class ResourceManager:
    logger: logging.Logger
    rai_config: RaiConfig

    def __init__(self, logger: logging.Logger, rai_config: RaiConfig):
        self.logger = logger
        self.rai_config = rai_config

    def get_rai_config(self):
        return self.rai_config

    def cleanup_resources(self) -> None:
        """
        Delete RAI DB and engine from RAI Config.
        :return:
        """
        rai.delete_database(self.logger, self.rai_config)
        rai.delete_engine(self.logger, self.rai_config)

    def create_database(self, deleted_db: bool = False, disable_ivm: bool = False) -> None:
        """
        Create RAI database. If `delete_db` is True then manager should delete previous db with the same name if it
         does exist before creation. `disable_ivm` indicate if manager need to disable IVM for a new db.
        :param deleted_db:      delete database flag
        :param disable_ivm:     disable ivm flag
        :return:
        """
        if deleted_db:
            self.deleted_database()
        rai.create_database(self.logger, self.rai_config)
        if disable_ivm:
            self.logger.info(f"Disabling IVM for `{self.rai_config.database}`")
            rai.execute_query(self.logger, self.rai_config, q.DISABLE_IVM, readonly=False)

    def deleted_database(self) -> None:
        """
        Delete RAI database if it does exist.
        :return:
        """
        if rai.database_exist(self.logger, self.rai_config):
            rai.delete_database(self.logger, self.rai_config)

    def create_engine(self, size: str = "XS") -> None:
        """
        Create RAI engine if it doesn't exist.
        :param size:    RAI engine size
        :return:
        """
        if not rai.engine_exist(self.logger, self.rai_config):
            rai.create_engine(self.logger, self.rai_config, size)

    def provision_engine(self, size: str) -> None:
        """
        Provision RAI engine from RAI config. Delete old engine if it does exist.
        :param size:    RAI engine size
        :return:
        """
        self.logger.info(f"Provision engine `{self.rai_config.engine}`")
        if rai.engine_exist(self.logger, self.rai_config):
            rai.delete_engine(self.logger, self.rai_config)
        rai.create_engine(self.logger, self.rai_config, size)

    @staticmethod
    def init(logger: logging.Logger, engine, database, env_vars: dict[str, Any] = MappingProxyType({})):
        logger = logger.getChild("workflow_resource_manager")
        rai_config = rai.get_config(engine, database, env_vars)
        return ResourceManager(logger, rai_config)
