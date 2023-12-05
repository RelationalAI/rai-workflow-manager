import logging
import sys
import random
import dataclasses
from typing import List, Iterable

from workflow import utils
from workflow.common import FileType, Export, Source, ContainerType, AzureConfig
from workflow.constants import IMPORT_CONFIG_REL, FILE_LOAD_RELATION

# Static queries
DISABLE_IVM = "def insert:relconfig:disable_ivm = true"

DELETE_REFRESHED_SOURCES_DATA = """
    def delete:source_catalog(r, p_i, data...) {
        resources_data_to_delete(r, p_i) and
        source_catalog(r, p_i, data...)
    }
    def delete:source_catalog[r] = source_catalog[r], resources_data_to_delete(r)
    def delete:simple_source_catalog[r] = simple_source_catalog[r], resources_data_to_delete(r)
    
    def delete:declared_sources_to_delete = declared_sources_to_delete
    def delete:resources_data_to_delete = resources_data_to_delete
"""


@dataclasses.dataclass
class QueryWithInputs:
    query: str
    inputs: dict


def load_json(relation: str, data) -> QueryWithInputs:
    return QueryWithInputs(f"def config:data = data\n" f"def insert:{relation} = load_json[config]", {"data": data})


def install_model(models: dict) -> QueryWithInputs:
    queries_inputs = {}
    rand_uint = random.randint(0, sys.maxsize)

    index = 0
    query = ""
    for name in models:
        input_name = f"input_{str(rand_uint)}_{index}"
        query += f"def delete:rel:catalog:model[\"{name}\"] = rel:catalog:model[\"{name}\"]\n" \
                 f"def insert:rel:catalog:model[\"{name}\"] = {input_name}\n"
        queries_inputs[input_name] = models[name]
        index += 1

    return QueryWithInputs(query, queries_inputs)


def populate_source_configs(sources: List[Source]) -> str:
    source_config_csv = "\n".join([source.to_paths_csv() for source in sources])
    data_formats_csv = "\n".join([source.to_formats_csv() for source in sources])
    container_types_csv = "\n".join([source.to_container_type_csv() for source in sources])

    chunk_partitioned_sources = list(filter(lambda source: source.is_chunk_partitioned, sources))
    simple_sources = list(
        filter(lambda source: not source.is_chunk_partitioned and not source.is_date_partitioned, sources))
    date_partitioned_sources = list(filter(lambda source: source.is_date_partitioned, sources))

    return f"""
        def delete:source_declares_resource(r, c, p) {{
            declared_sources_to_delete(r, p) and
            source_declares_resource(r, c, p)
        }}
        
        def resource_config[:data] = \"\"\"{source_config_csv}\"\"\"
        def resource_config[:syntax, :header_row] = -1
        def resource_config[:syntax, :header] = (1, :Relation); (2, :Container); (3, :Path)
        def resource_config[:schema, :Relation] = "string"
        def resource_config[:schema, :Container] = "string"
        def resource_config[:schema, :Path] = "string"
        def source_config_csv = load_csv[resource_config]
        def insert:source_declares_resource(r, c, p) =
            exists(i : 
                source_config_csv(:Relation, i, r) and 
                source_config_csv(:Container, i, c) and
                source_config_csv(:Path, i, p)
            )

        def input_format_config[:data] = \"\"\"{data_formats_csv}\"\"\"
        def input_format_config[:syntax, :header_row] = -1
        def input_format_config[:syntax, :header] = (1, :Relation); (2, :InputFormatCode)
        def input_format_config[:schema, :Relation] = "string"
        def input_format_config[:schema, :InputFormatCode] = "string"
        def input_format_config_csv = load_csv[input_format_config]
        def insert:source_has_input_format(r, p) =
            exists(i : input_format_config_csv(:Relation, i, r) and input_format_config_csv(:InputFormatCode, i, p))
        
        def container_type_config[:data] = \"\"\"{container_types_csv}\"\"\"
        def container_type_config[:syntax, :header_row] = -1
        def container_type_config[:syntax, :header] = (1, :Relation); (2, :ContainerType)
        def container_type_config[:schema, :Relation] = "string"
        def container_type_config[:schema, :ContainerType] = "string"
        def container_type_config_csv = load_csv[container_type_config]
        def insert:source_has_container_type(r, t) =
            exists(i : container_type_config_csv(:Relation, i, r) and container_type_config_csv(:ContainerType, i, t))

        {f"def insert:simple_source_relation = {_to_rel_literal_relation([source.relation for source in simple_sources])}" if len(simple_sources) > 0 else ""}
        {f"def insert:chunk_partitioned_source_relation = {_to_rel_literal_relation([source.relation for source in chunk_partitioned_sources])}" if len(chunk_partitioned_sources) > 0 else ""}
        {f"def insert:date_partitioned_source_relation = {_to_rel_literal_relation([source.relation for source in date_partitioned_sources])}" if len(date_partitioned_sources) > 0 else ""}
    """


def discover_reimport_sources(sources: List[Source], expired_sources: List[tuple[str, str]], force_reimport: bool,
                              force_reimport_not_chunk_partitioned: bool) -> str:
    date_partitioned_src_cfg_csv = ""
    expired_sources_src_cfg_csv = ""
    for src in sources:
        date_partitioned_src_cfg_csv += f"{src.to_chunk_partitioned_paths_csv()}\n"
    for src in expired_sources:
        expired_sources_src_cfg_csv += f"{src[0]},{src[1]}\n"
    return f"""
        def force_reimport = {"true" if force_reimport else "false"}
        def force_reimport_not_chunk_partitioned = {"true" if force_reimport_not_chunk_partitioned else "false"}

        def resource_config = new_source_config
        def resource_config[:data] = \"\"\"{date_partitioned_src_cfg_csv}\"\"\"
        def new_source_config_csv = load_csv[resource_config]
        
        def expired_resource_config = expired_source_config
        def expired_resource_config[:data] = \"\"\"{expired_sources_src_cfg_csv}\"\"\"
        def expired_source_config_csv = load_csv[expired_resource_config]
        
        def insert:declared_sources_to_delete = resource_to_invalidate
        def insert:declared_sources_to_delete(rel, path) = part_resource_to_invalidate(rel, _, path)
        
        def insert:resources_data_to_delete = resources_to_delete
    """


def load_resources(logger: logging.Logger, config: AzureConfig, resources, src) -> QueryWithInputs:
    rel_name = src["source"]

    file_stype_str = src["file_type"]
    file_type = FileType[file_stype_str]
    src_type = ContainerType.from_source(src)

    if 'is_multi_part' in src and src['is_multi_part'] == 'Y':
        if file_type == FileType.CSV or file_type == FileType.JSONL:
            if src_type == ContainerType.LOCAL:
                logger.info(f"Loading {len(resources)} shards from local files")
                return _local_load_multipart_query(rel_name, file_type, resources)
            elif src_type == ContainerType.AZURE:
                logger.info(f"Loading {len(resources)} shards from Azure files")
                return QueryWithInputs(_azure_load_multipart_query(rel_name, file_type, resources, config), {})
        else:
            logger.error(f"Unknown file type {file_stype_str}")
    else:
        if src_type == ContainerType.LOCAL:
            logger.info("Loading from local file")
            return _local_load_simple_query(rel_name, resources[0]["uri"], file_type)
        elif src_type == ContainerType.AZURE:
            logger.info("Loading from Azure file")
            return QueryWithInputs(_azure_load_simple_query(rel_name, resources[0]["uri"], file_type, config), {})


def get_snapshot_expiration_date(snapshot_binding: str, date_format: str) -> str:
    rai_date_format = utils.to_rai_date_format(date_format)
    return f"""
    def output(valid_until) {{
        batch_source:relation(cfg_src, "{snapshot_binding}") and
        batch_source:snapshot_validity_days(cfg_src, validity_days) and
        source:relname(src, :{snapshot_binding}) and
        snapshot_date = source:spans[src] and
        valid_until = format_date[snapshot_date + Day[validity_days], "{rai_date_format}"]
        from cfg_src, src, snapshot_date, validity_days
    }}
    """


def export_relations_local(logger: logging.Logger, exports: List[Export]) -> str:
    query = ""
    for export in exports:
        if export.file_type == FileType.CSV:
            if export.meta_key:
                query += _export_meta_relation_as_csv_local(export)
            else:
                query += _export_relation_as_csv_local(export.relation)
        else:
            logger.warning(f"Unsupported export type: {export.file_type}")
    return query


def export_relations_to_azure(logger: logging.Logger, config: AzureConfig, exports: List[Export], end_date: str,
                              date_format: str) -> str:
    query = f"""
    def _credentials_config:integration:provider = "azure"
    def _credentials_config:integration:credentials:azure_sas_token = raw"{config.sas}"
    """
    for export in exports:
        if export.file_type == FileType.CSV:
            if export.meta_key:
                query += _export_meta_relation_as_csv_to_azure(config, export, end_date, date_format)
            else:
                query += _export_relation_as_csv_to_azure(config, export, end_date, date_format)
        else:
            logger.warning(f"Unsupported export type: {export.file_type}")
    return query


def init_workflow_steps(batch_config_name: str) -> str:
    return f"""
    def delete:batch_workflow_step:state_value(s, v) {{ 
        batch_workflow_step:workflow[s] . batch_workflow:name[:{batch_config_name}] and
        batch_workflow_step:state_value(s, v)
    }}
    def delete:batch_workflow_step:execution_time_value(s, v) {{ 
        batch_workflow_step:workflow[s] . batch_workflow:name[:{batch_config_name}] and
        batch_workflow_step:execution_time_value(s, v)
    }}
    def insert:batch_workflow_step:execution_time_value(s, v) {{ 
        batch_workflow_step:workflow[s] . batch_workflow:name[:{batch_config_name}] and
        v = 0.0
        
    }}
    def insert:batch_workflow_step:state_value(s, v) {{ 
        batch_workflow_step:workflow[s] . batch_workflow:name[:{batch_config_name}] and
        v = "INIT"
    }}
    """


def delete_relation(relation: str) -> str:
    return f"def delete:{relation} = {relation}"


def update_step_state(idt: str, state: str) -> str:
    return f"""
    def insert:batch_workflow_step:state_value(s in BatchWorkflowStep, v) {{
        s = uint128_hash_value_convert[parse_uuid["{idt}"]] and 
        v = "{state}"
    }}
    """


def update_execution_time(idt: str, execution_time: float) -> str:
    return f"""
    def insert:batch_workflow_step:execution_time_value(s in BatchWorkflowStep, v) {{
        s = uint128_hash_value_convert[parse_uuid["{idt}"]] and 
        v = {execution_time}
    }}
    """


def materialize(relations: List[str]) -> str:
    query = ""
    for relation in relations:
        query += f"def output:{relation} = count[{relation}]\n"
    return query


def output_json(relation: str) -> str:
    return f"def output = json_string[{relation}]"


def output_relation(relation: str) -> str:
    return f"def output = {relation}"


def _local_load_simple_query(rel_name: str, uri: str, file_type: FileType) -> QueryWithInputs:
    try:
        raw_data_rel_name = f"{rel_name}_data"
        data = utils.read(uri)
        query = f"def {IMPORT_CONFIG_REL}:{rel_name}:data = {raw_data_rel_name}\n" \
                f"{_simple_insert_query(rel_name, file_type)}\n"
        return QueryWithInputs(query, {raw_data_rel_name: data})
    except OSError as e:
        raise e


def _azure_load_simple_query(rel_name: str, uri: str, file_type: FileType, config: AzureConfig) -> str:
    return f"def {IMPORT_CONFIG_REL}:{rel_name}:integration:provider = \"azure\"\n" \
           f"def {IMPORT_CONFIG_REL}:{rel_name}:integration:credentials:azure_sas_token = raw\"{config.sas}\"\n" \
           f"def {IMPORT_CONFIG_REL}:{rel_name}:path = \"{uri}\"\n" \
           f"{_simple_insert_query(rel_name, file_type)}"


def _local_load_multipart_query(rel_name: str, file_type: FileType, parts) -> QueryWithInputs:
    raw_data_rel_name = f"{rel_name}_data"

    raw_text = ""
    part_indexes = ""
    inputs = {}
    for part in parts:
        try:
            part_idx = part["part_index"]
            data = utils.read(part["uri"])
            inputs[_indexed_literal(raw_data_rel_name, part_idx)] = data
            raw_text += _load_from_indexed_literal(raw_data_rel_name, part_idx)
            part_indexes += f"{part_idx}\n"
        except OSError as e:
            raise e

    insert_text = _multi_part_insert_query(rel_name, file_type)
    load_config = _multi_part_load_config_query(rel_name, file_type,
                                                _local_multipart_config_integration(raw_data_rel_name))

    query = f"{_part_index_relation(part_indexes)}\n" \
            f"{raw_text}\n" \
            f"{load_config}\n" \
            f"{insert_text}"

    return QueryWithInputs(query, inputs)


def _azure_load_multipart_query(rel_name: str, file_type: FileType, parts, config: AzureConfig) -> str:
    path_rel_name = f"{rel_name}_path"

    part_indexes = ""
    part_uri_map = ""
    for part in parts:
        part_idx = part["part_index"]
        part_uri = part["uri"]
        part_indexes += f"{part_idx}\n"
        part_uri_map += f"{part_idx},\"{part_uri}\"\n"

    insert_text = _multi_part_insert_query(rel_name, file_type)
    load_config = _multi_part_load_config_query(rel_name, file_type,
                                                _azure_multipart_config_integration(path_rel_name, config))

    return f"{_part_index_relation(part_indexes)}\n" \
           f"{_path_rel_name_relation(path_rel_name, part_uri_map)}\n" \
           f"{load_config}\n" \
           f"{insert_text}"


def _multi_part_load_config_query(rel_name: str, file_type: FileType, config_integration: str) -> str:
    schema = ""
    if file_type == FileType.CSV:
        schema = f"def schema = {IMPORT_CONFIG_REL}:{rel_name}:schema\n" \
                 f"def syntax:header = {IMPORT_CONFIG_REL}:{rel_name}:syntax:header"

    return f"""
bound {IMPORT_CONFIG_REL}:{rel_name}:schema
bound {IMPORT_CONFIG_REL}:{rel_name}:syntax:header
module {_config_rel_name(rel_name)}[i in part_indexes]
    {schema}
    {config_integration}
end
"""


def _local_multipart_config_integration(raw_data_rel_name: str) -> str:
    return f"def data = {raw_data_rel_name}[i]"


def _azure_multipart_config_integration(path_rel_name: str, config: AzureConfig) -> str:
    return f"def integration:provider = \"azure\"\n" \
           f"def integration:credentials:azure_sas_token = raw\"{config.sas}\"\n" \
           f"def path = {path_rel_name}[i]\n"


def _multi_part_insert_query(rel_name: str, file_type: FileType) -> str:
    conf_rel_name = _config_rel_name(rel_name)

    return f"def insert:source_catalog:{rel_name}[i] = {FILE_LOAD_RELATION[file_type]}[{conf_rel_name}[i]]"


def _simple_insert_query(rel_name: str, file_type: FileType) -> str:
    return f"def insert:simple_source_catalog:{rel_name} = {FILE_LOAD_RELATION[file_type]}[{IMPORT_CONFIG_REL}:{rel_name}]"


def _load_from_indexed_literal(raw_data_rel_name: str, index: int) -> str:
    return f"def {raw_data_rel_name}[{index}] = {_indexed_literal(raw_data_rel_name, index)}\n"


def _indexed_literal(raw_data_rel_name: str, index: int) -> str:
    return f"{raw_data_rel_name}_{index}"


def _config_rel_name(rel: str) -> str: return f"load_{rel}_config"


def _part_index_relation(part_index: str) -> str:
    return f"def part_index_config:schema:INDEX = \"int\"\n" \
           f"def part_index_config:data = \"\"\"\n" \
           f"INDEX\n" \
           f"{part_index}\n" \
           f"\"\"\"" \
           f"def part_indexes_csv = load_csv[part_index_config]\n" \
           f"def part_indexes = part_indexes_csv:INDEX[_]"


def _path_rel_name_relation(path_rel_name: str, part_uri_map: str) -> str:
    return f"def part_uri_map_config:schema:INDEX = \"int\"\n" \
           f"def part_uri_map_config:schema:URI = \"string\"\n" \
           f"def part_uri_map_config:data = \"\"\"\n" \
           f"INDEX,URI\n" \
           f"{part_uri_map}\n" \
           f"\"\"\"" \
           f"def part_uri_map_csv = load_csv[part_uri_map_config]\n" \
           f"def {path_rel_name}(i, u) {{ part_uri_map_csv:INDEX(row, i) and part_uri_map_csv:URI(row, u) from row }}"


def _export_relation_as_csv_local(rel_name) -> str:
    return f"def _export_csv_config:{rel_name} = export_config:{rel_name}\n" \
           f"def output:{rel_name} = csv_string[_export_csv_config:{rel_name}]"


def _export_meta_relation_as_csv_local(export: Export) -> str:
    rel_name = export.relation
    key_str = _to_rel_meta_key_as_seq(export)
    return f"""
    module _export_csv_config
        def {rel_name}[{key_str}] =
            export_config:{rel_name}[{key_str}], export_config:{rel_name}:meta_key({key_str})
    end
    def output:{rel_name}[{key_str}] = csv_string[_export_csv_config:{rel_name}[{key_str}]]
    """


def _export_relation_as_csv_to_azure(config: AzureConfig, export: Export, end_date: str, date_format: str) -> str:
    rel_name = export.relation
    export_path = f"{_compose_export_path(config, export, end_date, date_format)}/{rel_name}.csv"
    return f"""
    module _export_csv_config
        def {rel_name} = export_config:{rel_name}
        def {rel_name}:path = raw"{export_path}"
        def {rel_name} = _credentials_config
    end
    def export:{rel_name} = export_csv[_export_csv_config:{rel_name}]
    """


def _export_meta_relation_as_csv_to_azure(config: AzureConfig, export: Export, end_date: str, date_format: str) -> str:
    rel_name = export.relation
    postfix = _to_rel_meta_key_as_str(export)
    base_path = _compose_export_path(config, export, end_date, date_format)
    export_path = f"{base_path}/{rel_name}_{postfix}.csv"
    key_str = _to_rel_meta_key_as_seq(export)
    return f"""
    module _export_csv_config
        module {rel_name}
            def meta_key({key_str}) = export_config:{rel_name}:meta_key({key_str})
            def filename_postfix[{key_str}] = meta_key({key_str}), "{_to_rel_meta_key_as_str(export)}"
            def path[{key_str}] = meta_key({key_str}), "{export_path}"
    
            def config[keys...] = meta_key(keys...), {{
                :path, path[keys...] ;
                export_config:{rel_name}[keys...] ;
                _credentials_config
            }}
        end
    end
    def export:{rel_name}[{key_str}] = export_csv[_export_csv_config:{rel_name}:config[{key_str}]],
        _export_csv_config:{rel_name}:meta_key[{key_str}]
    """


def _compose_export_path(config: AzureConfig, export: Export, end_date: str, date_format: str) -> str:
    account_url = f"azure://{config.account}.blob.core.windows.net"
    date_path = utils.get_date_path(end_date, date_format, export.offset_by_number_of_days)
    if export.meta_key:
        postfix = _to_rel_meta_key_as_str(export)
        return f"{account_url}/{config.container}/{config.data_path}/{export.relative_path}_{postfix}/{date_path}"
    else:
        return f"{account_url}/{config.container}/{config.data_path}/{export.relative_path}/{date_path}"


def _to_rel_literal_relation(xs: Iterable[str]) -> str:
    return "{ " + " ; ".join([f"\"{x}\"" for x in xs]) + " }"


def _to_rel_meta_key_as_seq(export: Export) -> str:
    key_values = range(0, len(export.meta_key))
    return ", ".join([f"_v{i}" for i in key_values])


def _to_rel_meta_key_as_str(export: Export) -> str:
    key_values = range(0, len(export.meta_key))
    return "_".join([f"%(_v{i})" for i in key_values])
