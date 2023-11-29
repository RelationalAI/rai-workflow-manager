import time
import logging
import sys
import tomli
from types import MappingProxyType

import cli.args
import cli.logger
import workflow.constants
import workflow.manager
import workflow.common
import workflow.utils
import workflow.executor


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
        logger.info(f"Activating batch with config from '{args.batch_config}'")
        start_time = time.time()
        # load batch config as json string
        batch_config_json = workflow.utils.read_config(args.batch_config)
        # create engine if it doesn't exist
        resource_manager.add_engine(args.engine_size)
        # Skip infrastructure setup during recovery
        if not args.recover and not args.recover_step:
            # Create db and disable IVM in case of enabled flag
            resource_manager.create_database(args.drop_db, args.disable_ivm, args.source_database)
        # Init workflow executor
        parameters = {
            workflow.constants.REL_CONFIG_DIR: args.rel_config_dir,
            workflow.constants.START_DATE: args.start_date,
            workflow.constants.END_DATE: args.end_date,
            workflow.constants.FORCE_REIMPORT: args.force_reimport,
            workflow.constants.FORCE_REIMPORT_NOT_CHUNK_PARTITIONED: args.force_reimport_not_chunk_partitioned,
            workflow.constants.COLLAPSE_PARTITIONS_ON_LOAD: args.collapse_partitions_on_load
        }
        config = workflow.executor.WorkflowConfig(env_config, workflow.common.BatchConfig(args.batch_config_name,
                                                                                          batch_config_json),
                                                  args.recover, args.recover_step, args.selected_steps, parameters,
                                                  args.step_timeout_dict)
        executor = workflow.executor.WorkflowExecutor.init(logger, config, resource_manager, factories, models)
        end_time = time.time()
        executor.run()
        # Print execution time information
        executor.print_timings()
        logger.info(f"Infrastructure setup time is {workflow.utils.format_duration(end_time - start_time)}")
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

