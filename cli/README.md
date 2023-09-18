# RAI Workflow Framework CLI

This Command-Line Interface (CLI) is designed to provide an easy and interactive way to execute batch configurations with the RAI Workflow Framework. 

## Quick Start
1. Create a batch configuration (ex. `poc.json`) file using the syntax and structure outlined in the [RAI Workflow Framework README](../workflow/README.md).
2. Add `rai-workflow-manager` as dependency to your `requirements.txt` file:
```txt
rai-workflow-manager==0.0.9
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
5. Run the following command to execute the batch configuration:
```bash
python main.py \
  --run-mode local \
  --dev-data-dir <path>/data \
  --rel-config-dir <path>/rel \
  --batch-config poc.json \
  --env-config loader.toml \
  --engine <engine> \
  --database <database> \
  --output-root ./output
```
where `<engine>`, `<database>` are the names of some RAI resources to use, `<path>` is the path to the directory containing directory with data and rel sources.

## CLI Arguments
| Description                                                                                                                                                                             | CLI argument                             | Is required | Default value           | Parameter Type           | Recognized Values                                   |
|:----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|------------------------------------------|-------------|-------------------------|--------------------------|-----------------------------------------------------|
| Relative path to batch configuration json                                                                                                                                               | `--batch-config`                         | `True`      |                         | `String`                 |                                                     |
| The name of the batch configuration json                                                                                                                                                | `--batch-config-name`                    | `False`     | `default`               | `String`                 |                                                     |
| RAI Cloud database name                                                                                                                                                                 | `--database`                             | `True`      |                         | `String`                 |                                                     |
| RAI Cloud source database name for clone                                                                                                                                                | `--source-database`                      | `False`     |                         | `String`                 |                                                     |
| RAI Cloud engine name                                                                                                                                                                   | `--engine`                               | `True`      |                         | `String`                 |                                                     |
| The size of RAI engine                                                                                                                                                                  | `--engine-size`                          | `False`     | `XS`                    | `String`                 | `['XS', 'S', 'M', 'L', 'XL']`                       |
| Run mode                                                                                                                                                                                | `--run-mode`                             | `True`      |                         | `String`                 | `['local', 'remote']`                               |
| Model earliest date to consider                                                                                                                                                         | `--start-date`                           | `False`     |                         | `String`                 | format `YYYYmmdd`                                   |
| Model latest date to consider                                                                                                                                                           | `--end-date`                             | `False`     |                         | `String`                 | format `YYYYmmdd`                                   |
| Directory containing dev data                                                                                                                                                           | `--dev-data-dir`                         | `False`     | `../data`               | `String`                 |                                                     |
| Directory containing rel config files to install                                                                                                                                        | `--rel-config-dir`                       | `False`     | `../rel`                | `String`                 |                                                     |
| Path to `loader.toml`                                                                                                                                                                   | `--env-config`                           | `False`     | `../config/loader.toml` | `String`                 |                                                     |
| When loading each multi-part source, <br/>load all partitions (and shards) in one transaction                                                                                           | `--collapse-partitions-on-load`          | `False`     | `True`                  | `Bool`                   |                                                     |
| Output folder path for local run mode                                                                                                                                                   | `--output-root`                          | `False`     | `../../output`          | `String`                 |                                                     |
| Logging level for cli                                                                                                                                                                   | `--log-level`                            | `False`     | `INFO`                  | `String`                 | `['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL']` |
| Drop database before workflow run, or not                                                                                                                                               | `--drop-db`                              | `False`     | `False`                 | `BooleanOptionalAction`  | `True` in case argument presents                    |
| Remove RAI engine and database after run or not                                                                                                                                         | `--cleanup-resources`                    | `False`     | `False`                 | `Bool`                   |                                                     |
| Disable IVM for RAI database                                                                                                                                                            | `--disable-ivm`                          | `False`     | `True`                  | `Bool`                   |                                                     |
| Recover a batch run starting from a FAILED step                                                                                                                                         | `--recover`                              | `False`     | `False`                 | `BooleanOptionalAction`  | `True` in case argument presents                    |
| Recover a batch run starting from specified step                                                                                                                                        | `--recover-step`                         | `False`     |                         | `String`                 | The value should be a step name.                    |
| Selected workflow steps to run. <br/>Note: if `--recover` is enabled then workflow reruns specified steps                                                                               | `--selected-steps`                       | `False`     |                         | `List[String]`           |                                                     |
| Parameter to set http retries for rai SDK                                                                                                                                               | `--rai-sdk-http-retries`                 | `False`     | `3`                     | `Int`                    | The value should be >= 0.                           |
| Force reimport of sources which are date-partitioned (both chunk and NOT chunk-partitioned) with in `--start-date` & `--end-date` range and all sources which are NOT date-partitioned. | `--force-reimport`                       | `False`     | `False`                 | `BooleanOptionalAction`  | `True` in case argument presents                    |
| Force reimport of sources which are NOT chunk-partitioned. If it's a date-partitioned source, it will be re-imported with in `--start-date` & `--end-date` range.                       | `--force-reimport-not-chunk-partitioned` | `False`     | `False`                 | `BooleanOptionalAction`  | `True` in case argument presents                    |
