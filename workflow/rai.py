import logging
import re
import json
import time
from typing import Dict, Any
from urllib.error import HTTPError
from railib import api, config

from workflow import query as q
from workflow.constants import RAI_PROFILE, RAI_PROFILE_PATH, RAI_SDK_HTTP_RETRIES
from workflow.common import RaiConfig


def get_config(engine: str, database: str, env_vars: dict[str, Any]) -> RaiConfig:
    """
    Create RAI config for given parameters
    :param engine:          RAI engine
    :param database:        RAI database
    :param env_vars:        Env vars dictionary
    :return: Env config
    """
    rai_profile = env_vars.get(RAI_PROFILE, "default")
    rai_profile_path = env_vars.get(RAI_PROFILE_PATH, "~/.rai/config")
    retries = env_vars.get(RAI_SDK_HTTP_RETRIES, 3)
    ctx = api.Context(**config.read(fname=rai_profile_path, profile=rai_profile), retries=retries)
    return RaiConfig(ctx=ctx, engine=engine, database=database)


def load_json(logger: logging.Logger, rai_config: RaiConfig, relation: str, json_data: str) -> None:
    """
    Load json into RAI DB
    :param logger:      logger
    :param rai_config:  RAI config
    :param relation:    relation for insert
    :param json_data:   json data string
    :return:
    """
    logger.info(f"Loading json as '{relation}'")
    logger.debug(f"Json content: '{json_data}'")
    query_model = q.load_json(relation, json_data)
    execute_query(logger, rai_config, query_model.query, query_model.inputs, readonly=False)


def create_engine(logger: logging.Logger, rai_config: RaiConfig, size: str = "XS") -> None:
    """
    Create RAI engine specified in RAI config.
    Waits until provision has finished.
    :param logger:      logger
    :param rai_config:  RAI config
    :param size:        RAI engine size
    :return:
    """
    logger.info(f"Creating engine `{rai_config.engine}`")
    api.create_engine_wait(rai_config.ctx, rai_config.engine, size)


def delete_engine(logger: logging.Logger, rai_config: RaiConfig) -> None:
    """
    Delete RAI engine specified in RAI config.
    :param logger:      logger
    :param rai_config:  RAI config
    :return:
    """
    logger.info(f"Deleting engine `{rai_config.engine}`")
    api.delete_engine(rai_config.ctx, rai_config.engine)
    # make sure that engine was deleted
    api.poll_with_specified_overhead(
        lambda: not engine_exist(logger, rai_config),
        overhead_rate=0.2,
        timeout=10 * 60,
    )


def engine_exist(logger: logging.Logger, rai_config: RaiConfig) -> bool:
    """
    Check if RAI engine specified in RAI config does exist on RAI Cloud.
    :param logger:      logger
    :param rai_config:  RAI config
    :return: `True` if the engine does exist otherwise `False`
    """
    logger.info(f"Check if engine `{rai_config.engine}` exist")
    return bool(api.get_engine(rai_config.ctx, rai_config.engine))


def create_database(logger: logging.Logger, rai_config: RaiConfig, source_db=None) -> None:
    """
    Create RAI DB specified in RAI config. The HTTP error 409(Conflict) swallowed since DB already exists.
    :param logger:      logger
    :param rai_config:  RAI config
    :param source_db:   source DB to clone from
    :return:
    """
    logger.info(f"Creating database `{rai_config.database}`")
    if source_db:
        logger.info(f"Use `{source_db}` database for clone")
    try:
        api.create_database(rai_config.ctx, rai_config.database, source_db)
    except HTTPError as e:
        if e.status == 409:
            logger.info(f"Database '{rai_config.database}' already exists")
        else:
            raise e


def delete_database(logger: logging.Logger, rai_config: RaiConfig) -> None:
    """
    Delete RAI DB specified in RAI config.
    :param logger:      logger
    :param rai_config:  RAI config
    :return:
    """
    logger.info(f"Deleting database `{rai_config.database}`")
    try:
        api.delete_database(rai_config.ctx, rai_config.database)
    except HTTPError as e:
        raise e


def database_exist(logger: logging.Logger, rai_config: RaiConfig) -> bool:
    """
    Check if RAI DB specified in RAI config does exist
    :param logger:      logger
    :param rai_config:  RAI config
    :return: `True` if DB exists otherwise `False`
    """
    logger.info(f"Check if db `{rai_config.database}` exist")
    return bool(api.get_database(rai_config.ctx, rai_config.database))


def install_models(logger: logging.Logger, rai_config: RaiConfig, models: dict) -> None:
    """
    Install Rel model into RAI DB specified in RAI config.
    :param logger:      logger
    :param rai_config:  RAI config
    :param models:      RAI models to install
    :return:
    """
    logger.info("Installing models")

    query_model = q.install_model(models)
    execute_query(logger, rai_config, query_model.query, query_model.inputs, False, False)


def execute_query(logger: logging.Logger, rai_config: RaiConfig, query: str, inputs: dict = None, readonly: bool = True,
                  ignore_problems: bool = False) -> api.TransactionAsyncResponse:
    """
    Execute Rel query using DB and engine from RAI config.
    :param logger:          logger
    :param rai_config:      RAI config
    :param query:           Rel query
    :param inputs:          Rel query inputs
    :param readonly:        Parameter to specify transaction type: Read/Write.
    :param ignore_problems: Ignore SDK problems if any
    :return: SDK response
    """
    try:
        logger.info(f"Execute query: database={rai_config.database} engine={rai_config.engine} readonly={readonly}")
        start_time = int(time.time())
        txn = api.exec_async(rai_config.ctx, rai_config.database, rai_config.engine, query, readonly, inputs)
        logger.info(f"Execute query: transaction id - {txn.transaction['id']}")

        # in case of if short-path, return results directly, no need to poll for state
        if not (txn.results is None):
            return txn

        logger.info(f"Execute query: polling for transaction with id - {txn.transaction['id']}")
        rsp = api.TransactionAsyncResponse()
        txn = api.get_transaction(rai_config.ctx, txn.transaction["id"])

        api.poll_with_specified_overhead(
            lambda: api.is_txn_term_state(api.get_transaction(rai_config.ctx, txn["id"])["state"]),
            overhead_rate=0.2,
            start_time=start_time
        )

        rsp.transaction = api.get_transaction(rai_config.ctx, txn["id"])
        rsp.metadata = api.get_transaction_metadata(rai_config.ctx, txn["id"])
        rsp.problems = api.get_transaction_problems(rai_config.ctx, txn["id"])
        rsp.results = api.get_transaction_results(rai_config.ctx, txn["id"])

        _assert_problems(logger, rsp, ignore_problems)
        return rsp
    except HTTPError as e:
        raise e


def execute_relation_json(logger: logging.Logger, rai_config: RaiConfig, relation: str,
                          ignore_problems: bool = False) -> Dict:
    """
    Execute Rel query with output relation as a json string and parse the output.
    :param logger:          logger
    :param rai_config:      RAI config
    :param relation:        Rel relations
    :param ignore_problems: Ignore SDK problems if any
    :return: parsed json string
    """
    rsp = execute_query(logger, rai_config, q.output_json(relation), ignore_problems=ignore_problems)
    return _parse_json_string(rsp)


def execute_query_csv(logger: logging.Logger, rai_config: RaiConfig, query: str, ignore_problems: bool = False) -> Dict:
    """
    Execute query and parse the output as CSV.
    :param logger:          logger
    :param rai_config:      RAI config
    :param query:           Rel query
    :param ignore_problems: Ignore SDK problems if any
    :return: parsed CSV output
    """
    rsp = execute_query(logger, rai_config, query, ignore_problems=ignore_problems)
    return _parse_csv_string(rsp)


def execute_query_take_single(logger: logging.Logger, rai_config: RaiConfig, query: str, readonly: bool = True,
                              ignore_problems: bool = False) -> any:
    """
    Execute query and take the first result.
    :param logger:          logger
    :param rai_config:      RAI config
    :param query:           Rel query
    :param readonly:        Parameter to specify transaction type: Read/Write
    :param ignore_problems: Ignore SDK problems if any
    """
    rsp = execute_query(logger, rai_config, query, readonly=readonly, ignore_problems=ignore_problems)
    if not rsp.results:
        logger.info(f"Query returned no results: {query}")
        return None
    return rsp.results[0]['table'].to_pydict()["v1"][0]


def _assert_problems(logger: logging.Logger, rsp: api.TransactionAsyncResponse, ignore_problems: bool):
    problems_has_error = _handle_problems(logger, rsp.problems)
    txn_state = rsp.transaction.get('state')
    if txn_state != 'COMPLETED' or (problems_has_error and not ignore_problems):
        # todo: create a hierarchy of exceptions
        raise AssertionError(
            f"Transaction did not complete successfully or has problems: {txn_state}, id: {rsp.transaction['id']}")


def _handle_problems(logger: logging.Logger, problems: list) -> bool:
    problems_has_error = False
    for problem in problems:
        if 'is_error' in problem and problem['is_error'] is True or \
                'is_exception' in problem and problem['is_exception'] is True:
            problems_has_error = True
            logger.error(problem)
        else:
            logger.warning(problem)
    return problems_has_error


def _parse_json_string(rsp: api.TransactionAsyncResponse) -> Dict:
    """
    Parse the output for a json_string query ie
        def output = json_string[...]
    """
    if not rsp.results:
        return {}
    pa_table = rsp.results[0]['table']
    data = pa_table.to_pydict()
    return json.loads(data["v1"][0])


def _parse_csv_string(rsp: api.TransactionAsyncResponse) -> Dict:
    """
    Parse the output for a csv_string query ie
        def output:{rel_name} = csv_string[...]
    """
    resp = {}
    rel_pattern = r'^/:output/:(.*)/String$'
    for result in rsp.results:
        match = re.search(rel_pattern, result['relationId'])
        if match:
            relation_id = match.group(1)
            data = result['table'].to_pydict()["v1"][0]
            resp[relation_id] = data
    return resp
