import logging
from types import MappingProxyType

from workflow import rai, constants
from workflow import query as q
from workflow.common import EnvConfig, RaiConfig, BatchConfig
from workflow.exception import RetryException
from workflow.manager import ResourceManager
from workflow.rest import SemanticSearchRestClient
from workflow.utils import build_models, get_common_model_relative_path, call_with_overhead


def init(logger: logging.Logger, env_config: EnvConfig, batch_config: BatchConfig, resource_manager: ResourceManager,
         models: dict[str, str] = MappingProxyType({})):
    logger = logger.getChild("activator")
    rai_config = resource_manager.get_rai_config()

    # Install common model for workflow manager
    core_models = build_models(constants.COMMON_MODEL, get_common_model_relative_path(__file__))
    extended_models = {**core_models, **models}
    logger.info("Installing RWM common models...")
    rai.install_models(logger, rai_config, env_config, extended_models)

    rest_client = SemanticSearchRestClient(logger, env_config.semantic_search_base_url,
                                           env_config.semantic_search_pod_prefix)
    logger.info(f"Starting layer service with '{env_config.semantic_search_pod_prefix}' pod prefix")
    startup_rsp = rest_client.startup(rai_config, env_config.rai_cloud_account)
    _wait_startup_complete(logger, rai_config, env_config, rest_client, startup_rsp["startupId"])

    wf_resp = rest_client.create_workflow(rai_config, env_config.rai_cloud_account, batch_config.content)
    update_query = q.update_workflow_idt(batch_config, wf_resp["workflowId"])
    rai.execute_query(logger, rai_config, env_config, update_query, readonly=False)


def shutdown(logger: logging.Logger, env_config: EnvConfig, resource_manager: ResourceManager) -> None:
    logger = logger.getChild("activator")
    rai_config = resource_manager.get_rai_config()
    rest_client = SemanticSearchRestClient(logger, env_config.semantic_search_base_url,
                                           env_config.semantic_search_pod_prefix)
    rest_client.shutdown(rai_config, env_config.rai_cloud_account)


def _wait_startup_complete(logger: logging.Logger, rai_config: RaiConfig, env_config: EnvConfig,
                           rest_client: SemanticSearchRestClient, startup_id: str) -> None:
    if startup_id == "":
        raise Exception("Semantic layer startup wasn't triggered")
    account_name = env_config.rai_cloud_account
    try:
        call_with_overhead(
            f=lambda: _is_startup_completed(logger, rai_config, rest_client,
                                            account_name, int(startup_id)),
            logger=logger,
            overhead_rate=0.5,
            timeout=1800  # 30 min
        )
    except RetryException:
        raise Exception("Semantic layer startup timeout")


def _is_startup_completed(logger: logging.Logger, rai_config: RaiConfig, rest_client: SemanticSearchRestClient,
                          account_name: str, startup_id: int) -> bool:
    result = rest_client.get_startup_result(rai_config, account_name, startup_id)
    logger.debug(f"Semantic layer startup result: {result}")
    return result["isStartupInProgress"] is False
