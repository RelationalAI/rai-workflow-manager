# RAI Workflow Framework CLI

This Command-Line Interface (CLI) is designed to provide an easy and interactive way to execute batch configurations with the RAI Workflow Framework. 

## Quick Start
1. Create a batch configuration (ex. `poc.json`) file using the syntax and structure outlined in the [RAI Workflow Framework README](../workflow/README.md).
2. Add `rai-workflow-manager` as dependency to your `requirements.txt` file:
```txt
rai-workflow-manager==0.0.17
```
3. Build the project:
```bash
pyenv install 3.9
pyenv local 3.9
pip install --upgrade pip
pip install virtualenv
python -m virtualenv venv
source ./venv/bin/activate
pip install -r requirements.txt
```
4. Create main.py file with the following content:
```python
import cli.runner

if __name__ == "__main__":
    cli.runner.start()
```
5. Create `loader.toml` file with the following content:
```toml
[[container]]
name="input"
type="local"
data_path="./data"

[[container]]
name="export"
type="local"
data_path="./output"
```
6. Run the following command to execute the batch configuration:
```bash
python main.py \
  --rel-config-dir <path>/rel \
  --batch-config poc.json \
  --env-config loader.toml \
  --engine <engine> \
  --database <database>
```
where `<engine>`, `<database>` are the names of some RAI resources to use
## `loader.toml` Configuration
The `loader.toml` file is used to specify static properties for the RAI Workflow Framework. It contains the following properties:

| Description                                                                                 | Property               |
|:--------------------------------------------------------------------------------------------|------------------------|
| RAI profile.                                                                                | `rai_profile`          |
| Path to RAI config.                                                                         | `rai_profile_path`     |
| HTTP retries for RAI sdk in case of errors. (Can be overridden by CLI argument)             | `rai_sdk_http_retries` |
| A list of containers to use for loading and exporting data.                                 | `container`            |
| The name of the container.                                                                  | `container.name`       |
| The type of the container. Supported types: `local`, `azure`, `snowflake`(only data import) | `container.type`       |
| The path in the container.                                                                  | `container.data_path`  |
| Remote container account                                                                    | `container.account`    |
| Remote container SAS token.                                                                 | `container.sas`        |
| User for remote container.                                                                  | `container.user`       |
| Password for remote container.                                                              | `container.password`   |
| User role for remote container (e.g. Snowflake user role).                                  | `container.role`       |
| Database for remote container.                                                              | `container.database`   |
| Schema for remote container.                                                                | `container.schema`     |
| Warehouse for Snowflake container.                                                          | `container.warehouse`  |

### Azure container example
```toml
[[container]]
name="input"
type="azure"
data_path="input"
account="account_name"
sas="sas_token"
container="container_name"
```
### Snowflake container example
```toml
[[container]]
name="input"
type="snowflake"
account="account"
user="use"
password="password"
role="snowflake role"
warehouse="warehouse"
database="database"
schema="schema"
```
### Local container example
```toml
[[container]]
name="input"
type="local"
data_path="./data"
```
## CLI Arguments
| Description                                                                                                                                                                             | CLI argument                             | Is required | Default value           | Parameter Type           | Recognized Values                                                                                                                     |
|:----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|------------------------------------------|-------------|-------------------------|--------------------------|---------------------------------------------------------------------------------------------------------------------------------------|
| Relative path to batch configuration json                                                                                                                                               | `--batch-config`                         | `True`      |                         | `String`                 |                                                                                                                                       |
| The name of the batch configuration json                                                                                                                                                | `--batch-config-name`                    | `False`     | `default`               | `String`                 |                                                                                                                                       |
| RAI Cloud database name                                                                                                                                                                 | `--database`                             | `True`      |                         | `String`                 |                                                                                                                                       |
| RAI Cloud source database name for clone                                                                                                                                                | `--source-database`                      | `False`     |                         | `String`                 |                                                                                                                                       |
| RAI Cloud engine name                                                                                                                                                                   | `--engine`                               | `True`      |                         | `String`                 |                                                                                                                                       |
| The size of RAI engine                                                                                                                                                                  | `--engine-size`                          | `False`     | `XS`                    | `String`                 | `['XS', 'S', 'M', 'L', 'XL']`                                                                                                         |
| Model earliest date to consider                                                                                                                                                         | `--start-date`                           | `False`     |                         | `String`                 | format `YYYYmmdd`                                                                                                                     |
| Model latest date to consider                                                                                                                                                           | `--end-date`                             | `False`     |                         | `String`                 | format `YYYYmmdd`                                                                                                                     |
| Directory containing rel config files to install                                                                                                                                        | `--rel-config-dir`                       | `False`     | `../rel`                | `String`                 |                                                                                                                                       |
| Path to `loader.toml`                                                                                                                                                                   | `--env-config`                           | `False`     | `../config/loader.toml` | `String`                 |                                                                                                                                       |
| When loading each multi-part source, <br/>load all partitions (and shards) in one transaction                                                                                           | `--collapse-partitions-on-load`          | `False`     | `True`                  | `BooleanOptionalAction`  | `True` - `--collapse-partitions-on-load`, `False` - `--no-collapse-partitions-on-load`, no argument - default value                   |
| Logging level for cli                                                                                                                                                                   | `--log-level`                            | `False`     | `INFO`                  | `String`                 | `['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL']`                                                                                   |
| Drop database before workflow run, or not                                                                                                                                               | `--drop-db`                              | `False`     | `False`                 | `BooleanOptionalAction`  | `True` - `--drop-db`, `False` - `--no-drop-db`, no argument - default value                                                           |
| Remove RAI engine and database after run or not                                                                                                                                         | `--cleanup-resources`                    | `False`     | `False`                 | `BooleanOptionalAction`  | `True` - `--cleanup-resources`, `False` - `--no-cleanup-resources`, no argument - default value                                       |
| Remove RAI engine after run or not                                                                                                                                                      | `--cleanup-engine`                       | `False`     | `False`                 | `BooleanOptionalAction`  | `True` - `--cleanup-engine`, `False` - `--no-cleanup-engine`, no argument - default value                                             |
| Remove RAI database after run or not                                                                                                                                                    | `--cleanup-db`                           | `False`     | `False`                 | `BooleanOptionalAction`  | `True` - `--cleanup-db`, `False` - `--no-cleanup-db`, no argument - default value                                                     |
| Disable IVM for RAI database                                                                                                                                                            | `--disable-ivm`                          | `False`     | `True`                  | `BooleanOptionalAction`  | `True` - `--disable-ivm`, `False` - `--no-disable-ivm`, no argument - default value                                                   |
| Recover a batch run starting from a FAILED step                                                                                                                                         | `--recover`                              | `False`     | `False`                 | `BooleanOptionalAction`  | `True` - `--recover`, `False` - `--no-recover`, no argument - default value                                                           |
| Recover a batch run starting from specified step                                                                                                                                        | `--recover-step`                         | `False`     |                         | `String`                 | The value should be a step name.                                                                                                      |
| Selected workflow steps to run. <br/>Note: if `--recover` is enabled then workflow reruns specified steps                                                                               | `--selected-steps`                       | `False`     |                         | `List[String]`           |                                                                                                                                       |
| Parameter to set http retries for rai SDK                                                                                                                                               | `--rai-sdk-http-retries`                 | `False`     | `3`                     | `Int`                    | The value should be >= 0.                                                                                                             |
| Force reimport of sources which are date-partitioned (both chunk and NOT chunk-partitioned) with in `--start-date` & `--end-date` range and all sources which are NOT date-partitioned. | `--force-reimport`                       | `False`     | `False`                 | `BooleanOptionalAction`  | `True` - `--force-reimport`, `False` - `--no-force-reimport`, no argument - default value                                             |
| Force reimport of sources which are NOT chunk-partitioned. If it's a date-partitioned source, it will be re-imported with in `--start-date` & `--end-date` range.                       | `--force-reimport-not-chunk-partitioned` | `False`     | `False`                 | `BooleanOptionalAction`  | `True` - `--force-reimport-not-chunk-partitioned`, `False` - `--no-force-reimport-not-chunk-partitioned`, no argument - default value |
