import logging

import snowflake.connector

from workflow import constants
from workflow.utils import call_with_overhead_async
from workflow.common import SnowflakeConfig, RaiConfig


def begin_data_sync(logger: logging.Logger, snowflake_config: SnowflakeConfig, rai_config: RaiConfig, resources, src):
    conn = __get_connection(snowflake_config)
    cursor = conn.cursor()
    logger = logger.getChild("snowflake")

    destination_rel = src['source']
    source_table = resources[0]['uri']
    database = rai_config.database
    engine = rai_config.engine
    # Start data stream commands
    commands = (
        f"CALL RAI.use_rai_database('{database}');",
        f"CALL RAI.use_rai_engine('{engine}');",
        f"CALL RAI.create_data_stream('{source_table}', '{database}', 'simple_source_catalog, :{destination_rel}');"
    )
    try:
        for command in commands:
            logger.info(f"Executing Snowflake command: `{command.strip()}`")
            cursor.execute(command)
    except Exception as e:
        cursor.close()
        conn.close()
        raise e


async def await_data_sync(logger: logging.Logger, snowflake_config: SnowflakeConfig, resources):
    source_table = resources[0]['uri']
    conn = __get_connection(snowflake_config)
    cursor = conn.cursor()
    logger = logger.getChild("snowflake")
    # Wait for data sync finish
    try:
        logger.info(f"Wait for Snowflake data sync finish for `{source_table}`...")
        await call_with_overhead_async(
            f=lambda: sync_finished(logger, cursor, source_table),
            logger=logger,
            overhead_rate=0.5,
            timeout=30 * 60,  # 30 min
            first_delay=10,  # 10 sec since it can take some time to start Job on Snowflake by Ingestion Service
            max_delay=55  # 55 sec since snowflake warehouse can be suspended after 60 sec of inactivity
        )
    except Exception as e:
        raise e
    finally:
        # Clean up data stream after sync
        cursor.execute(f"CALL RAI.delete_data_stream('{source_table}')")
        cursor.close()
        conn.close()


def sync_finished(logger: logging.Logger, cursor, source_table: str):
    cursor.execute(f"CALL RAI.get_data_stream_status('{source_table}');")
    properties = cursor.fetchall()
    key_value_pairs = {stream[0]: stream[1] for stream in properties}
    health_status = key_value_pairs.get(constants.SNOWFLAKE_STREAM_HEALTH_STATUS)
    if health_status != constants.SNOWFLAKE_HEALTHY_STREAM_STATUS:
        raise Exception(f"Snowflake sync for `{source_table}` has failed. Health status: {health_status}")
    if key_value_pairs.get(constants.SNOWFLAKE_SYNC_STATUS) == constants.SNOWFLAKE_FINISHED_SYNC_STATUS:
        synced_rows = key_value_pairs.get(constants.SNOWFLAKE_TOTAL_ROWS)
        logger.info(f"Snowflake sync finished for `{source_table}` has finished. Synced row: {synced_rows}")
        return True
    return False


def __get_connection(config: SnowflakeConfig):
    """
    This function will get a connection to snowflake.
    """
    # connect to snowflake
    return snowflake.connector.connect(
        user=config.user,
        password=config.password,
        account=config.account,
        role=config.role,
        warehouse=config.warehouse,
        database=config.database,
        schema=config.schema,
    )
