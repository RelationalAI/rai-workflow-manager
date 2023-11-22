from schema import Schema, Optional, And, Use
from types import MappingProxyType
from workflow.constants import CONFIGURE_SOURCES, INSTALL_MODELS, LOAD_DATA, MATERIALIZE, EXPORT, EXECUTE_COMMAND
from workflow.common import FileType


class Validator:
    schemas: dict[str, Schema]

    def __init__(self, schemas: dict[str, Schema] = MappingProxyType({})):
        self.schemas = {**STEP_SCHEMAS, **schemas}

    def validate(self, data):
        Schema({"workflow": [WorkflowStepSchema({}, self)]}).validate(data)


class WorkflowStepSchema(Schema):

    def __init__(self, schema, validator: Validator, error=None, ignore_extra_keys=False, name=None, description=None,
                 as_reference=False):
        super().__init__(schema, error, ignore_extra_keys, name, description, as_reference)
        self.validator = validator

    def validate(self, data, **kwargs):
        step_type = data.get("type", None)
        self.validator.schemas.get(step_type, Schema(step_schema, ignore_extra_keys=True)).validate(data, **kwargs)


step_schema = {
    "type": str,
    "name": str,
    Optional("engineSize"): str
}

load_data_step_schema = Schema(step_schema)

materialize_step_schema = Schema({
    **step_schema,
    "materializeJointly": bool,
    "relations": [str],
})

install_model_step_schema = Schema({
    **step_schema,
    "modelFiles": [str]
})

execute_command_step_schema = Schema({
    **step_schema,
    "command": str
})

export_step_schema = Schema({
    **step_schema,
    "exportJointly": bool,
    "dateFormat": str,
    "defaultContainer": str,
    "exports": [{
        "type": str,
        "configRelName": str,
        "relativePath": str,
        Optional("container"): str,
        Optional("snapshotBinding"): str,
        Optional("offsetByNumberOfDays"): And(int, lambda n: 0 <= n),
        Optional("metaKey"): [str]
    }]
})

configure_sources_step_schema = Schema({
    **step_schema,
    "configFiles": [str],
    "defaultContainer": str,
    "sources": [{
        "relation": str,
        "relativePath": str,
        "inputFormat": And(str, Use(str.upper), lambda s: s in FileType.__members__),
        Optional("container"): str,
        Optional("isChunkPartitioned"): bool,
        Optional("future"): bool,
        Optional("isDatePartitioned"): bool,
        Optional("loadsNumberOfDays"): And(int, lambda n: 0 <= n),
        Optional("offsetByNumberOfDays"): And(int, lambda n: 0 <= n),
        Optional("snapshotValidityDays"): And(int, lambda n: 0 <= n),
        Optional("extensions"): [str]
    }]
})

STEP_SCHEMAS = MappingProxyType(
    {
        CONFIGURE_SOURCES: configure_sources_step_schema,
        INSTALL_MODELS: install_model_step_schema,
        LOAD_DATA: load_data_step_schema,
        MATERIALIZE: materialize_step_schema,
        EXPORT: export_step_schema,
        EXECUTE_COMMAND: execute_command_step_schema
    }
)
