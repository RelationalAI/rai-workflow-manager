
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
    "batch_config/workflow/steps/materialize.rel"
]

COMMON_MODEL_RELATIVE_PATH = "/../rel"

CONFIG_BASE_RELATION = "batch:config"

DATE_FORMAT = "%Y%m%d"

DATE_PREFIX = "data_dt="  # TODO use pattern?

BLOB_PAGE_SIZE = 500

# RAI constants
IMPORT_CONFIG_REL = "import_config"

MISSED_RESOURCES_REL = "missing_resources_json"
WORKFLOW_JSON_REL = "workflow_json"
BATCH_CONFIG_REL = "batch:config"

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
AZURE_ACCOUNT = "account"
AZURE_CONTAINER = "container"
AZURE_DATA_PATH = "data_path"
AZURE_SAS = "sas"
LOCAL_DATA_PATH = "data_path"

# Step parameters
REL_CONFIG_DIR = "rel_config_dir"
START_DATE = "start_date"
END_DATE = "end_date"
FORCE_REIMPORT = "force_reimport"
FORCE_REIMPORT_NOT_CHUNK_PARTITIONED = "force_reimport_not_chunk_partitioned"
COLLAPSE_PARTITIONS_ON_LOAD = "collapse_partitions_on_load"
