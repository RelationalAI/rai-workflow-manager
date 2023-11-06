# RAI Workflow Manager

This framework is designed to simplify the process of managing and executing complex workflows using the RAI database. With the RAI Workflow Manager, you can easily set up batch configurations, define and execute workflow steps, recover batch execution in case of failures, track execution time.

# Table of Contents

- [Build project](#build-project)
- [Batch Configuration](#batch-configuration)
  - [Common step properties](#common-step-properties)
  - [Supported Steps](#supported-steps)
    - [Configure Sources](#configure-sources)
    - [Install Model](#install-model)
    - [Load Data](#load-data)
    - [Materialize](#materialize)
    - [Export](#export)
  - [Snowflake integration](#snowflake-integration)
  - [Source partitioning](#source-partitioning)
    - [Partition Source Naming](#partition-source-naming)
    - [Part Index](#part-index)
- [Framework extension](#framework-extension)
  - [Custom workflow steps](#custom-workflow-steps)
  
# Build project
```bash
pyenv install 3.9
pyenv local 3.9
pip install --upgrade pip
pip install virtualenv
python -m virtualenv venv
source ./venv/bin/activate
pip install -r requirements.txt
```

# Batch Configuration

The RAI Workflow Manager uses batch configurations to define the steps of your workflow. A batch configuration is a JSON file that outlines the sequence of steps.
The order of the steps in the batch configuration is important, as the RAI Workflow Manager executes the steps in the order they are defined in the batch configuration.

## Common step properties

* `name` (required) The unique identifier of the workflow step. It is used to track the execution of the step and to recover the execution in case of failures.
* `type` (required) Used to determine which workflow step to execute. The list of supported steps is described in the [Supported Steps](#supported-steps) section.
* `engineSize` (optional) Identifies the size of the RAI engine to use for the step. If not specified, the default engine size is used.

## Supported Steps

### Configure Sources

Steps of this type are used to configure sources which workflow manager will use during [Load Data](#load-data) step.
* `configFiles`(required) is used to specify the configuration files to install. Leave empty if no configuration files are needed.
* `defaultContainer`(required) is used to specify the default container for sources input.
* `sources`(required) is used to specify the sources to configure.
  * `relation`(required) is used to specify the name of the relation to which the source should be uploaded.
  * `isChunkPartitioned`(optional) is used to specify whether the source is chunk partitioned(split on chunks). By default `False`.
  * `relativePath`(required) is used to specify the relative path of the source on Blob storage or in data on file system.
  * `inputFormat`(required) is used to specify the input format of the source. The supported input formats are `csv`, `jsonl`.
  * `isDatePartitioned`(optional) is used to specify is partitioned by date. By default `False`.
  * `container`(optional) is used to specify the container for particular source. If not specified, the `defaultContainer` will be used.
  * `extensions`(optional) is used to specify the extensions of the source which will be associated with the `inputFormat`. If not specified, the `inputFormat` will be used default extensions are used.
  * `loadsNumberOfDays`(optional) is used to specify the number of days to load.
  * `offsetByNumberOfDays`(optional) is used to specify the number of days to offset the current (end) date by.
  * `snapshotValidityDays`(optional) is used to specify the number of days a snapshot is valid for. Currently only supported for `csv` sources.
```json
{
  "type": "ConfigureSources",
  "name": "ConfigureSources",
  "configFiles": [
    "source_configs/my_config.rel"
  ],
  "defaultContainer": "input",
  "sources": [
    {
      "relation": "master_data",
      "container": "azure_input",
      "relativePath": "master_source/data",
      "inputFormat": "csv"
    },
    {
      "relation": "devices",
      "isChunkPartitioned": true,
      "isDatePartitioned": true,
      "relativePath": "source/devices_info",
      "inputFormat": "jsonl",
      "extensions": [
        "json",
        "jsonl"
      ],
      "loadsNumberOfDays": 60
    }
  ]
}
```

### Install Model

Steps of this type are used to install models into the RAI database. 
* `modelFiles`(required) is used to specify the model files to install.
```json
{
  "type": "InstallModels",
  "name": "InstallModels1",
  "modelFiles": [
    "data/devices.rel",
    "model/store.rel"
  ]
}
```

### Load Data

Steps of this type are used to load data into the RAI database.
```json
{
  "type": "LoadData",
  "name": "LoadData"
}
```
In this step, data from all configured sources is loaded into two relations:
* `simple_source_catalog` - no date partitioning, no chunking. Typically populated by a single, static file.
* `source_catalog` - date partitioned and chunked. Populated by daily incremental batches.
For both relations, you access the data using the value of the `relation` field of the source in the [Configure Sources](#configure-sources) step.

### Materialize

Steps of this type are used to materialize relations in the RAI database.
* `relations`(required) is used to specify the relations to materialize.
* `materializeJointly`(required) is used to specify whether the relations should be materialized jointly or separately.
```json
{
  "type": "Materialize",
  "name": "Materialize",
  "relations": [
    "account:device",
    "account:fraud_detected_on"
  ],
  "materializeJointly": true
}
```

### Export
Steps of this type are used to export data from RAI database.
* `exportJointly`(required) is used to specify whether the relations should be exported jointly or separately.
* `dateFormat`(required) is used to specify date format for export folder.
* `defaultContainer`(required) is used to specify the default container for export.
* `exports`(required) is used to specify relations to export.
  * `type`(required) is used to specify the type of export. The supported types are `csv`.
  * `configRelName`(required) is used to specify the name of the relation which configures the export.
  * `relativePath`(required) is used to specify the relative path of the export on Blob storage or in data on file system.
  * `container`(optional) is used to specify the container for particular export. If not specified, the `defaultContainer` will be used.
  * `snapshotBinding`(optional) is used to specify the name of the source which is bound to the export. If specified, the export will be skipped if the snapshot is still valid.
  * `metaKey`(optional) is used to specify the meta-key for the export. If specified, the export will be specialized by the meta-key.

```json
{
  "type": "Export",
  "name": "Export",
  "exportJointly": false,
  "dateFormat": "%Y%m%d",
  "defaultContainer": "export",
  "exports": [
    {
      "type": "csv",
      "configRelName": "account_journey_csv",
      "relativePath": "account_journey"
    },
    {
      "type": "csv",
      "container": "azure_output",
      "configRelName": "device_seen_snapshot_csv",
      "relativePath": "device_seen_snapshot",
      "snapshotBinding": "device_seen_snapshot"
    },
    {
      "type": "csv",
      "configRelName": "meta_exports",
      "relativePath": "meta_exports",
      "metaKey": [
        "Symbol"
      ]
    }
  ]
}
```
#### Common config options:

* `data` relation contains the well-defined (:col, key..., val) data to be exported;
* `syntax:header` relation contains the header of the CSV file (Int, Symbol);
* `partition_size` defines max size of the exported files in MB, if above threshold RAI partitions it in multiple files `filename_{part_nr}.csv`

#### Basic exports
The former needs to be used when a simple export config is passed as shown below:
```rel
module high_risk_zipcodes_csv
    def data = shipped_zip_stats

    def syntax:header = {
        1, :zip_id ;
        2, :touched ;
        3, :sold ;
        ...
    }

    ...
end
```
The corresponding JSON configuration might look like this:
```json
{
  "type": "csv",
  "configRelName": "high_risk_zipcodes_csv",
  "relativePath": "high_risk_zipcodes"
}
```

#### Snapshot bindings to the input sources

The workflow manager supports snapshots of the input sources. A snapshot may be valid for a certain period of time, after which a new
snapshot is required. The steps to configure a snapshot are described below.

This case involves a snapshot binding to the input sources. The most basic example would look like this, an entry in the `exports` array:
```json
{
  "type": "csv",
  "configRelName": "device_seen_snapshot_csv",
  "relativePath": "device_seen_snapshot",
  "snapshotBinding": "device_seen_snapshot"
}
```
The configuration shown above would require a corresponding source configured with `snapshotValidityDays` field specified, telling
the workflow manager how long the snapshot is valid for. `snapshotBinding` should point to the source name. During the export, if
workflow manager encounters a snapshot binding, it will look for the validity of the current snapshot. If still valid, the export
will be skipped.

Example source config:
```json
{
  "relation": "device_seen_snapshot",
  "isChunkPartitioned": true,
  "isDatePartitioned": true,
  "relativePath": "device_seen_snapshot",
  "inputFormat": "csv",
  "loadsNumberOfDays": 1,
  "offsetByNumberOfDays": 1,
  "snapshotValidityDays": 3
}
```

#### Meta-exports

This case involves a complex export config, like a module configuring multiple exports at the same time. The most basic
example would look like this:
```rel
module meta_exports[dim]
    def data = meta_exports_data[dim]

    def syntax:header = {
        dim = :foo, {  // notice the filter by `dim`
            1, :foo ;
            2, :bar
        };
        dim = :bar, {  // notice the filter by `dim`
            1, :one ;
            2, :two ;
            3, :three
        }
    }
end
def meta_exports:meta_key = first[meta_exports_data]
```
where it's expected that `dim` is of type `Symbol` and the data relation is specialized by a finite number of values
which must be put into the `meta_key` relation. In this example the arity of the meta-key is one, however it may be
arbitrary.

The above would require a JSON config like:
```json
{
  "type": "csv",
  "configRelName": "meta_exports",
  "relativePath": "meta_exports",
  "metaKey": [ "Symbol" ]
}
```
Note: to ensure it's possible to specialize the configs we require the type(s) of the meta-key passed in the config.
Right now the only supported one is `Symbol`.


A more interesting example is shown below:
```rel
    module one_hop[x, y]
        def data = one_hop_exports[x, y]

        // configures the partitioner to export CSV files in partitions of the size (MB):
        def partition_size = 512
    end
    // define a meta-key for the export
    def one_hop:meta_key(x, y) = exists one_hop_exports[x, y]
```
We chose an arbitrary order of the columns, not passing the `syntax:header` config in this case. The JSON config might
look like the one shown below:
```json
{
  "type": "csv",
  "configRelName": "one_hop",
  "relativePath": "one_hop",
  "metaKey": [ "Symbol", "Symbol" ]
}
```
## Snowflake Integration
Workflow manager supports Snowflake as a data source container only for data ingestion. 
### Integration Details
Workflow Manager uses RAI integration for data sync from Snowflake. Workflow Manager creates a data stream for each source in batch config with Snowflake container. Integration Service creates an ingestion engine per rai account with prefix `ingestion-engine-*` and uses this engine for data ingestion. Relation for data ingestion: `simple_source_catalog`. Once data sync is completed, Workflow Manager deletes the data stream. 

**Note:** Workflow Manager is not responsible for creating and deleting an ingestion engine. The ingestion engine is not deleted automatically after data sync.   
### Configure RAI Integration
To use Snowflake as a data source container, you need to configure Snowflake using following guides:
* [RAI Integration for Snowflake: Quick Start for Administrators](https://docs.relational.ai/preview/snowflake/quickstart-admin)
* [RAI Integration for Snowflake: Quick Start for Users](https://docs.relational.ai/preview/snowflake/quickstart-user)
### Configure Snowflake Container
Snowflake container configuration is defined in this section: [Snowflake container example](../cli/README.md#snowflake-container-example). 
## Source Partitioning
Workflow manager supports 2 types of source partitioning: date partitioning and chunk partitioning. Sources can be date partitioned and chunk partitioned at the same time.
All chunk partitioned source have `isChunkPartitioned: true` and all date partitioned source have `isDatePartitioned: true` in the [Configure Sources](#configure-sources) step.
### Partition Source Naming
Partitions should meet the criteria described below otherwise RAI Workflow Manager will not be able to load data from the source.
#### Date Partitioned Source
Sources partitioned by date should have a date in the file path or name. Date should be in the format `yyyymmdd`.
Regexp for date partitioned source must have `<date>` group and must be defined in `part_resource_date_pattern` relation. Example:
```rel
def part_resource_date_pattern = "^(.+)/data_dt=(?<date>[0-9]+)/(.+).(csv|json|jsonl)$"
```
#### Chunk Partitioned Source
Chunk partitioned sources should have a chunk index in the file name or path. Chunk index should be `int` value.
Regexp for chunk partitioned source must have `<shard>` or `<rai_shard>` group and must be defined in `part_resource_index_pattern` relation. Example:
```rel
def part_resource_index_pattern = "^(.+)/part-(?<shard>[0-9])-(.+).(csv|json|jsonl)$"
```
### Part Index
Part index is `int` value that is used to identify a particular part of the source. Data for each part is stored in `source_catalog` relation. 
The second element of this relation is Part Index value. If a user wants to load data from a date partitioned source, he needs to set multiplier value to `part_resource_index_multiplier` relation. 
Example: 
```rel
def part_resource_index_multiplier = 1000000
```
#### Part Index Calculation 
* For date partitioned source: `part_index = "date int value" * part_resource_index_multiplier`.
  
  **Example**: 
  ```text
  Input:
  date = "20220105"
  part_resource_index_multiplier = 100000
  Result:
  part_index = 2022010500000
  ```
* For chunk partitioned source: `part_index = chunk_index`
* For date and chunk partitioned source: `part_index = "date int value" * part_resource_index_multiplier + chunk_index`.
  
  **Example**:
  ```text
  Input: 
  date = "20220105"
  part_resource_index_multiplier = 100000
  chunk_index = 1
  Result:
  part_index=2022010500001
  ```
# Framework extension

## Custom workflow steps
TBD
