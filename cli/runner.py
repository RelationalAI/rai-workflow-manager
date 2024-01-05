import time
import logging
import sys
import tomli
from types import MappingProxyType

import cli.args
import cli.logger
import cli.common
import workflow.constants
import workflow.manager
import workflow.common
import workflow.utils
import workflow.executor
import workflow.exception
import workflow.rai
from workflow import semantic_layer_service


def start(factories: dict[str, workflow.executor.WorkflowStepFactory] = MappingProxyType({}),
          models: dict[str, str] = MappingProxyType({})):
    # parse arguments
    args = cli.args.parse()
    # configure logger
    log_config = cli.logger.LogConfiguration(logging.getLevelName(args.log_level), args.log_rotation,
                                             args.log_file_size, args.log_file_name)
    logger = cli.logger.configure(log_config)
    try:
        with open(args.env_config, "rb") as fp:
            loader_config = tomli.load(fp)
    except OSError as e:
        logger.exception("Failed to load 'loader.toml' config.", e)
        sys.exit(1)
    loader_config[workflow.constants.RAI_SDK_HTTP_RETRIES] = args.rai_sdk_http_retries
    # init env config
    env_config = workflow.common.EnvConfig.from_env_vars(loader_config)
    # init Workflow resource manager
    resource_manager = workflow.manager.ResourceManager.init(logger, args.engine, args.database, env_config)
    logger.info("Using: " + ",".join(f"{k}={v}" for k, v in vars(args).items()))
    try:
        start_time = time.time()
        # create engine if it doesn't exist
        resource_manager.add_engine(args.engine_size)
        if args.action == cli.common.CliAction.INIT:
            # load batch config as json string
            batch_config_json = workflow.utils.read_config(args.batch_config)
            # Create db and disable IVM in case of enabled flag
            resource_manager.create_database(args.drop_db, args.disable_ivm, args.source_database)
            semantic_layer_service.init(logger, env_config,
                                        workflow.common.BatchConfig(args.batch_config_name, batch_config_json),
                                        resource_manager, models)
        elif args.action == cli.common.CliAction.RUN:
            # Init workflow executor
            parameters = {
                workflow.constants.REL_CONFIG_DIR: args.rel_config_dir,
                workflow.constants.START_DATE: args.start_date,
                workflow.constants.END_DATE: args.end_date,
                workflow.constants.FORCE_REIMPORT: args.force_reimport,
                workflow.constants.FORCE_REIMPORT_NOT_CHUNK_PARTITIONED: args.force_reimport_not_chunk_partitioned,
                workflow.constants.COLLAPSE_PARTITIONS_ON_LOAD: args.collapse_partitions_on_load
            }
            config = workflow.executor.WorkflowConfig(env_config, args.batch_config_name, args.recover,
                                                      parameters, args.step_timeout_dict)
            executor = workflow.executor.WorkflowExecutor.init(logger, config, resource_manager, factories)
            end_time = time.time()
            executor.run()
            logger.info(
                f"Infrastructure setup time is {workflow.utils.format_duration((end_time - start_time) * 1000)}")
    except Exception as e:
        # Cleanup resources in case of any failure.
        logger.exception(e)
        sys.exit(1)
    finally:
        if args.cleanup_resources:
            resource_manager.cleanup_resources()
        else:
            if args.cleanup_db:
                resource_manager.delete_database()
            if args.cleanup_engine:
                resource_manager.cleanup_engines()

