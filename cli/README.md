# RAI Workflow Framework CLI

This Command-Line Interface (CLI) is designed to provide an easy and interactive way to execute batch configurations
with the RAI Workflow Framework.

## Quick Start

1. Create a batch configuration (ex. `poc.json`) file using the syntax and structure outlined in
   the [RAI Workflow Framework README](../workflow/README.md).
2. Install Python 3.9+ ([Install Python using pyenv](#install-python-using-pyenv)).
3. Install RWM:

```bash
pip install rai-workflow-manager
```

4. Build and run Semantic Layer Service if you want to use local
   instance: [Build Semantic Layer Service](https://github.com/RelationalAI/semantic-search?tab=readme-ov-file#running-the-semantic-search-service).
5. Create `loader.toml` file with the following content:

```toml
sematic_search_base_url = "<semantic-layer-url>"
rai_cloud_account = "<rai-cloud-sccount>"

[[container]]
name = "input"
type = "local"
data_path = "./data"

[[container]]
name = "export"
type = "local"
data_path = "./output"
```

Where `<sematic-layer-url>` - url to Semantic Layer Service (For local service
instance: http://localhost:8081), `<rai-cloud-sccount>` - RAI Cloud account name.

6. Execute the following command to init workflow from the batch configuration:

```bash
rwm --batch-config poc.json \
  --env-config loader.toml \
  --engine <engine> \
  --database <database> \
  --action init
```

where `<engine>`, `<database>` are the names of some RAI resources to use

7. Execute the following command to run workflow:

```bash
rwm --rel-config-dir ./rel \
  --env-config loader.toml \
  --engine <engine> \
  --database <database> \
  --action run
```

where `<engine>`, `<database>` are the names of some RAI resources to use

## `loader.toml` Configuration

The `loader.toml` file is used to specify static properties for the RAI Workflow Framework. It contains the following
properties:

| Description                                                                                                             | Property                               |
|:------------------------------------------------------------------------------------------------------------------------|----------------------------------------|
| RAI profile.                                                                                                            | `rai_profile`                          |
| Path to RAI config.                                                                                                     | `rai_profile_path`                     |
| HTTP retries for RAI sdk in case of errors. (Can be overridden by CLI argument)                                         | `rai_sdk_http_retries`                 |
| Enable check for multiple write txns in flight to avoid parallel writes initiated by other interactions with RAI engine | `fail_on_multiple_write_txn_in_flight` |
| Semantic Layer Service base url.                                                                                        | `sematic_search_base_url`              |
| RAI Cloud account name.                                                                                                 | `rai_cloud_account`                    |
| A list of containers to use for loading and exporting data.                                                             | `container`                            |
| The name of the container.                                                                                              | `container.name`                       |
| The type of the container. Supported types: `local`, `azure`, `snowflake`(only data import)                             | `container.type`                       |
| The path in the container.                                                                                              | `container.data_path`                  |
| Remote container account                                                                                                | `container.account`                    |
| Remote container SAS token.                                                                                             | `container.sas`                        |
| User for remote container.                                                                                              | `container.user`                       |
| Password for remote container.                                                                                          | `container.password`                   |
| User role for remote container (e.g. Snowflake user role).                                                              | `container.role`                       |
| Database for remote container.                                                                                          | `container.database`                   |
| Schema for remote container.                                                                                            | `container.schema`                     |
| Warehouse for Snowflake container.                                                                                      | `container.warehouse`                  |

### Azure container example

```toml
[[container]]
name = "input"
type = "azure"
data_path = "input"
account = "account_name"
sas = "sas_token"
container = "container_name"
```

### Snowflake container example

```toml
[[container]]
name = "input"
type = "snowflake"
account = "account"
user = "use"
password = "password"
role = "snowflake role"
warehouse = "warehouse"
database = "database"
schema = "schema"
```

### Local container example

```toml
[[container]]
name = "input"
type = "local"
data_path = "./data"
```

## CLI Arguments

| Description                                                                                                                                                                             | CLI argument                             | Action scope | Is required | Default value           | Parameter Type          | Recognized Values                                                                                                                     |
|:----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|------------------------------------------|--------------|-------------|-------------------------|-------------------------|---------------------------------------------------------------------------------------------------------------------------------------|
| CLI action                                                                                                                                                                              | `--action`                               |              | `True`      |                         | `String`                | `['init', run']`                                                                                                                      |
| Relative path to batch configuration json                                                                                                                                               | `--batch-config`                         | `init`       | `True`      |                         | `String`                |                                                                                                                                       |
| The name of the batch configuration json                                                                                                                                                | `--batch-config-name`                    |              | `False`     | `default`               | `String`                |                                                                                                                                       |
| RAI Cloud database name                                                                                                                                                                 | `--database`                             |              | `True`      |                         | `String`                |                                                                                                                                       |
| RAI Cloud source database name for clone                                                                                                                                                | `--source-database`                      | `init`       | `False`     |                         | `String`                |                                                                                                                                       |
| RAI Cloud engine name                                                                                                                                                                   | `--engine`                               |              | `True`      |                         | `String`                |                                                                                                                                       |
| The size of RAI engine                                                                                                                                                                  | `--engine-size`                          |              | `False`     | `XS`                    | `String`                | `['XS', 'S', 'M', 'L', 'XL']`                                                                                                         |
| Model earliest date to consider                                                                                                                                                         | `--start-date`                           | `run`        | `False`     |                         | `String`                | format `YYYYmmdd`                                                                                                                     |
| Model latest date to consider                                                                                                                                                           | `--end-date`                             | `run`        | `False`     |                         | `String`                | format `YYYYmmdd`                                                                                                                     |
| Directory containing rel config files to install                                                                                                                                        | `--rel-config-dir`                       | `run`        | `False`     | `../rel`                | `String`                |                                                                                                                                       |
| Path to `loader.toml`                                                                                                                                                                   | `--env-config`                           |              | `False`     | `../config/loader.toml` | `String`                |                                                                                                                                       |
| When loading each multi-part source, <br/>load all partitions (and shards) in one transaction                                                                                           | `--collapse-partitions-on-load`          | `run`        | `False`     | `True`                  | `BooleanOptionalAction` | `True` - `--collapse-partitions-on-load`, `False` - `--no-collapse-partitions-on-load`, no argument - default value                   |
| Logging level for cli                                                                                                                                                                   | `--log-level`                            |              | `False`     | `INFO`                  | `String`                | `['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL']`                                                                                   |
| Log rotation option. If `date` options is enabled RWM rotates logs each day. If `size` option is enabled RWM rotates log file when it reaches this size                                 | `--log-rotation`                         |              | `False`     | `date`                  | `String`                | `['date', 'size']`                                                                                                                    |
| Rotation log file size in Mb. RWM rotates log file when it reaches this size and `--log-rotation` is `size`                                                                             | `--log-file-size`                        |              | `False`     | `5`                     | `Int`                   |                                                                                                                                       |
| Log file name                                                                                                                                                                           | `--log-file-name`                        |              | `False`     | `rwm`                   | `String`                |                                                                                                                                       |
| Drop database before workflow run, or not                                                                                                                                               | `--drop-db`                              | `init`       | `False`     | `False`                 | `BooleanOptionalAction` | `True` - `--drop-db`, `False` - `--no-drop-db`, no argument - default value                                                           |
| Remove RAI engine and database after run or not                                                                                                                                         | `--cleanup-resources`                    |              | `False`     | `False`                 | `BooleanOptionalAction` | `True` - `--cleanup-resources`, `False` - `--no-cleanup-resources`, no argument - default value                                       |
| Remove RAI engine after run or not                                                                                                                                                      | `--cleanup-engine`                       |              | `False`     | `False`                 | `BooleanOptionalAction` | `True` - `--cleanup-engine`, `False` - `--no-cleanup-engine`, no argument - default value                                             |
| Remove RAI database after run or not                                                                                                                                                    | `--cleanup-db`                           |              | `False`     | `False`                 | `BooleanOptionalAction` | `True` - `--cleanup-db`, `False` - `--no-cleanup-db`, no argument - default value                                                     |
| Disable IVM for RAI database                                                                                                                                                            | `--disable-ivm`                          | `init`       | `False`     | `True`                  | `BooleanOptionalAction` | `True` - `--disable-ivm`, `False` - `--no-disable-ivm`, no argument - default value                                                   |
| Recover a batch run starting from a FAILED step                                                                                                                                         | `--recover`                              | `run`        | `False`     | `False`                 | `BooleanOptionalAction` | `True` - `--recover`, `False` - `--no-recover`, no argument - default value                                                           |
| Parameter to set http retries for rai SDK                                                                                                                                               | `--rai-sdk-http-retries`                 |              | `False`     | `3`                     | `Int`                   | The value should be >= 0.                                                                                                             |
| Parameter to set timeouts for steps                                                                                                                                                     | `--step-timeout`                         | `run`        | `False`     |                         | `String`                | The value should be key value pairs separated by comma. Value must have `int` type. Example: `--step-timeout "step1=10,step2=20"`     |
| Force reimport of sources which are date-partitioned (both chunk and NOT chunk-partitioned) with in `--start-date` & `--end-date` range and all sources which are NOT date-partitioned. | `--force-reimport`                       | `run`        | `False`     | `False`                 | `BooleanOptionalAction` | `True` - `--force-reimport`, `False` - `--no-force-reimport`, no argument - default value                                             |
| Force reimport of sources which are NOT chunk-partitioned. If it's a date-partitioned source, it will be re-imported with in `--start-date` & `--end-date` range.                       | `--force-reimport-not-chunk-partitioned` | `run`        | `False`     | `False`                 | `BooleanOptionalAction` | `True` - `--force-reimport-not-chunk-partitioned`, `False` - `--no-force-reimport-not-chunk-partitioned`, no argument - default value |

## Install Python using pyenv

```bash
pyenv install 3.9
pyenv local 3.9
pip install --upgrade pip
pip install virtualenv
python -m virtualenv venv
source ./venv/bin/activate
```