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
* `sources`(required) is used to specify the sources to configure.
  * `relation`(required) is used to specify the name of the relation to which the source should be uploaded.
  * `isChunkPartitioned`(optional) is used to specify whether the source is chunk partitioned(split on chunks). By default `False`.
  * `relativePath`(required) is used to specify the relative path of the source on Blob storage or in data on file system.
  * `inputFormat`(required) is used to specify the input format of the source. The supported input formats are `csv`, `jsonl`.
  * `isDatePartitioned`(optional) is used to specify is partitioned by date. By default `False`.
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
  "sources": [
    {
      "relation": "master_data",
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
#### Limitations
- We do not support chunk partitioning for sources which are not partitioned by date.

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
# Framework extension

## Custom workflow steps
TBD
