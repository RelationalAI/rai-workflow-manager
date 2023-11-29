
COMMON_MODEL = [
    "source_configs/config.rel",
    "source_configs/data_reload.rel",
    "batch_config/batch_config.rel",
    "batch_config/workflow/workflow.rel",
    "batch_config/workflow/steps/configure_sources.rel",
    "batch_config/workflow/steps/export.rel",
    "batch_config/workflow/steps/install_models.rel",
    "batch_config/workflow/steps/invoke_solver.rel",
    "batch_config/workflow/steps/load_data.rel",
    "batch_config/workflow/steps/materialize.rel",
    "batch_config/workflow/steps/execute_command.rel",
]

COMMON_MODEL_RELATIVE_PATH = "/../rel"

CONFIG_BASE_RELATION = "batch:config"

DATE_FORMAT = "%Y%m%d"

DATE_PREFIX = "data_dt="  # TODO use pattern?

BLOB_PAGE_SIZE = 500

# Step types
CONFIGURE_SOURCES = 'ConfigureSources'
INSTALL_MODELS = 'InstallModels'
LOAD_DATA = 'LoadData'
# not supported
INVOKE_SOLVER = 'InvokeSolver'
MATERIALIZE = 'Materialize'
EXPORT = 'Export'
EXECUTE_COMMAND = 'ExecuteCommand'

# RAI constants
IMPORT_CONFIG_REL = "import_config"

MISSED_RESOURCES_REL = "missing_resources_json"
RESOURCES_TO_DELETE_REL = "resources_data_to_delete_json"
WORKFLOW_JSON_REL = "workflow_json"
BATCH_CONFIG_REL = "batch:config"
DECLARED_DATE_PARTITIONED_SOURCE_REL = "declared_date_partitioned_source:json"

FILE_LOAD_RELATION = {
    "CSV": "load_csv",
    "JSONL": "load_jsonlines_general",
    "JSON": "load_json" # TODO: maybe load_json_general?
}
# Env config params
CONTAINER = "container"
CONTAINER_TYPE = "type"
CONTAINER_NAME = "name"
RAI_PROFILE = "rai_profile"
RAI_PROFILE_PATH = "rai_profile_path"
RAI_SDK_HTTP_RETRIES = "rai_sdk_http_retries"
FAIL_ON_MULTIPLE_WRITE_TXN_IN_FLIGHT = "fail_on_multiple_write_txn_in_flight"
SEMANTIC_SEARCH_BASE_URL = "sematic_search_base_url"
RAI_CLOUD_ACCOUNT = "rai_cloud_account"
# Generic container params
ACCOUNT_PARAM = "account"
USER_PARAM = "user"
PASSWORD_PARAM = "password"
SCHEMA_PARAM = "schema"
DATABASE_PARAM = "database"
CONTAINER_PARAM = "container"
DATA_PATH_PARAM = "data_path"
# Datasource specific params
AZURE_SAS = "sas"
SNOWFLAKE_ROLE = "role"
SNOWFLAKE_WAREHOUSE = "warehouse"

# Step parameters
REL_CONFIG_DIR = "rel_config_dir"
START_DATE = "start_date"
END_DATE = "end_date"
FORCE_REIMPORT = "force_reimport"
FORCE_REIMPORT_NOT_CHUNK_PARTITIONED = "force_reimport_not_chunk_partitioned"
COLLAPSE_PARTITIONS_ON_LOAD = "collapse_partitions_on_load"

# Snowflake constants

# Properties
SNOWFLAKE_SYNC_STATUS = "Data sync status"
SNOWFLAKE_STREAM_HEALTH_STATUS = "Data stream health"
SNOWFLAKE_TOTAL_ROWS = "Latest changes written to RAI - Total rows"
# Values
SNOWFLAKE_FINISHED_SYNC_STATUS = "\"Fully synced\""
SNOWFLAKE_HEALTHY_STREAM_STATUS = "\"Healthy\""
