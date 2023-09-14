import logging
import sys
import random
import dataclasses
from typing import List, Iterable

from workflow import utils
from workflow.common import EnvConfig, FileType, Export, Source, SourceType
from workflow.constants import IMPORT_CONFIG_REL, FILE_LOAD_RELATION

# Static queries
DISABLE_IVM = "def insert:relconfig:disable_ivm = true"


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

    simple_sources = list(filter(lambda source: not source.is_chunk_partitioned, sources))
    multipart_sources = list(filter(lambda source: source.is_chunk_partitioned, sources))
    date_partitioned_sources = list(filter(lambda source: source.is_date_partitioned, sources))

    return f"""
        def resource_config[:data] = \"\"\"{source_config_csv}\"\"\"
        def resource_config[:syntax, :header_row] = -1
        def resource_config[:syntax, :header] = (1, :Relation); (2, :Path)
        def resource_config[:schema, :Relation] = "string"
        def resource_config[:schema, :Path] = "string"

        def source_config_csv = load_csv[resource_config]

        def insert:source_declares_resource(r, p) =
            exists(i : source_config_csv(:Relation, i, r) and source_config_csv(:Path, i, p))

        def input_format_config[:data] = \"\"\"{data_formats_csv}\"\"\"

        def input_format_config[:syntax, :header_row] = -1
        def input_format_config[:syntax, :header] = (1, :Relation); (2, :InputFormatCode)
        def input_format_config[:schema, :Relation] = "string"
        def input_format_config[:schema, :InputFormatCode] = "string"

        def input_format_config_csv = load_csv[input_format_config]

        def insert:source_has_input_format(r, p) =
            exists(i : input_format_config_csv(:Relation, i, r) and input_format_config_csv(:InputFormatCode, i, p))

        {f"def insert:simple_relation = {_to_rel_literal_relation([source.relation for source in simple_sources])}" if len(simple_sources) > 0 else ""}
        {f"def insert:multi_part_relation = {_to_rel_literal_relation([source.relation for source in multipart_sources])}" if len(multipart_sources) > 0 else ""}
        {f"def insert:date_partitioned_source = {_to_rel_literal_relation([source.relation for source in date_partitioned_sources])}" if len(date_partitioned_sources) > 0 else ""}
    """


def discover_reimport_sources(sources: List[Source], force_reimport: bool, force_reimport_not_chunk_partitioned: bool) -> str:
    date_partitioned_src_cfg_csv = ""
    for src in sources:
        date_partitioned_src_cfg_csv += f"{src.to_chunk_partitioned_paths_csv()}\n"
    return f"""
        def force_reimport = {"true" if force_reimport else "false"}
        def force_reimport_not_chunk_partitioned = {"true" if force_reimport_not_chunk_partitioned else "false"}
    
        def resource_config[:data] = \"\"\"{date_partitioned_src_cfg_csv}\"\"\"
        def resource_config[:syntax, :header_row] = -1
        def resource_config[:syntax, :header] = (1, :Relation); (2, :Path); (3, :ChunkPartitioned)
        def resource_config[:schema, :Relation] = "string"
        def resource_config[:schema, :Path] = "string"
        def resource_config[:schema, :ChunkPartitioned] = "string"

        def source_config_csv = load_csv[resource_config]

        def chunk_partitioned_sources(rel, path, p_idx) {{
            source_config_csv(:Path, i, path) and
            source_config_csv(:Relation, i, rel) and
            source_config_csv(:ChunkPartitioned, i, "True") and
            p_idx = part_resource:parse_part_index[path]
            from i
        }}
        
        def simple_sources(rel, path) {{ 
            source_config_csv(:Path, i, path) and
            source_config_csv(:Relation, i, rel) and
            not chunk_partitioned_sources(rel, path, _)
            from i
        }}
        // TODO: add support for chunk partitioned sources which are not date partitioned
        /*
         * All simple sources are affected if they match with declared sources.
         */
        def potentially_affected_sources(rel, path) {{
            source_declares_resource(rel, path) and
            simple_sources(rel, path)
        }}
        /*
         * All chunk partitioned sources for given date are affected in case new sources have at least one partition in this date.
         */
        def potentially_affected_sources(rel, o_path, p_idx) {{
            chunk_partitioned_sources(rel, n_path, _) and
            res = relation:identifies[ rel_name:identifies[ ^RelName[rel] ] ] . source:declares and
            part_resource:hashes_to_date[res] = parse_date[ uri:parse_value[n_path, "date"], "yyyymmdd"] and
            part_resource:hashes_to_part_index(res, p_idx) and
            resource:id(res, ^URI[o_path])
            from n_path, res
        }}
        /*
         * Identify sources for replacement.
         */
        def part_resource_to_replace(rel, p_idx, path) {{
            not force_reimport and
            potentially_affected_sources(rel, path, p_idx) and
            not chunk_partitioned_sources(rel, path, p_idx)
        }}
        
        def part_resource_to_replace(rel, p_idx, path) {{
            force_reimport and
            potentially_affected_sources(rel, path, p_idx)
        }}
        
        def resource_to_replace(rel, o_path) {{
            force_reimport_not_chunk_partitioned and
            simple_sources(rel, o_path)
        }}
        
        def resource_to_replace(rel, o_path) {{
            force_reimport and
            simple_sources(rel, o_path)
        }}
        
        /*
         * Save resources that we are marked for data reimport.
         */
        // PRODUCT_LIMITATION: Message: Argument for specializing is more complex than the current staging implementation supports.
        def reverse_part_resource_to_replace(path, p_idx, rel) = part_resource_to_replace(rel, p_idx, path)
        // resource partitions to delete
        def insert:resources_to_delete(rel, val) {{
            rel = #(reverse_part_resource_to_replace[_, p_idx]) and
            p_idx = ^PartIndex[val]
            from p_idx
        }}
        // resources to delete
        def insert:resources_to_delete(rel) {{
            rel = #(transpose[resource_to_replace][_])
        }}
        /*
         * Clean up the declared resources that we are marked for data reimport.
         */
        def delete:source_declares_resource(rel, path) {{
            part_resource_to_replace(rel, _, path)
        }}
        def delete:source_declares_resource(rel, path) {{
            resource_to_replace(rel, path)
        }}
    """


def load_resources(logger: logging.Logger, env_config: EnvConfig, resources, src) -> str:
    rel_name = src["source"]

    file_stype_str = src["file_type"]
    file_type = FileType[file_stype_str]
    src_type = SourceType.from_source(src)

    if 'is_multi_part' in src and src['is_multi_part'] == 'Y':
        if file_type == FileType.CSV or file_type == FileType.JSONL:
            if src_type == SourceType.LOCAL:
                logger.info(f"Loading {len(resources)} shards from local files")
                return _local_load_multipart_query(rel_name, file_type, resources)
            elif src_type == SourceType.REMOTE:
                logger.info(f"Loading {len(resources)} shards from remote files")
                return _remote_load_multipart_query(rel_name, file_type, resources, env_config)
        else:
            logger.error(f"Unknown file type {file_stype_str}")
    else:
        if src_type == SourceType.LOCAL:
            logger.info("Loading from local file.")
            return _local_load_simple_query(rel_name, resources[0]["uri"], file_type)
        elif src_type == SourceType.REMOTE:
            logger.info("Loading from remote file.")
            return _remote_load_simple_query(rel_name, resources[0]["uri"], file_type, env_config)


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


def export_relations_remote(logger: logging.Logger, env_config: EnvConfig, exports: List[Export], end_date: str,
                            date_format: str) -> str:
    query = f"""
    def _credentials_config:integration:provider = "azure"
    def _credentials_config:integration:credentials:azure_sas_token = raw"{env_config.azure_export_sas}"
    """
    for export in exports:
        if export.file_type == FileType.CSV:
            if export.meta_key:
                query += _export_meta_relation_as_csv_remote(env_config, export, end_date, date_format)
            else:
                query += _export_relation_as_csv_remote(env_config, export, end_date, date_format)
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


def _local_load_simple_query(rel_name: str, uri: str, file_type: FileType) -> str:
    try:
        raw_data_rel_name = f"{rel_name}_data"
        data = utils.read(uri)
        return f"{_load_from_literal(data, raw_data_rel_name)}\n" \
               f"def {IMPORT_CONFIG_REL}:{rel_name}:data = {raw_data_rel_name}\n" \
               f"{_simple_insert_query(rel_name, file_type)}\n"
    except OSError as e:
        raise e


def _remote_load_simple_query(rel_name: str, uri: str, file_type: FileType, env_config: EnvConfig) -> str:
    return f"def {IMPORT_CONFIG_REL}:{rel_name}:integration:provider = \"azure\"\n" \
           f"def {IMPORT_CONFIG_REL}:{rel_name}:integration:credentials:azure_sas_token = raw\"{env_config.azure_import_sas}\"\n" \
           f"def {IMPORT_CONFIG_REL}:{rel_name}:path = \"{uri}\"\n" \
           f"{_simple_insert_query(rel_name, file_type)}"


def _local_load_multipart_query(rel_name: str, file_type: FileType, parts) -> str:
    raw_data_rel_name = f"{rel_name}_data"

    raw_text = ""
    part_indexes = ""
    for part in parts:
        try:
            part_idx = part["part_index"]
            data = utils.read(part["uri"])
            raw_text += _load_from_indexed_literal(data, raw_data_rel_name, part_idx)
            part_indexes += f"{part_idx}\n"
        except OSError as e:
            raise e

    insert_text = _multi_part_insert_query(rel_name, file_type)
    load_config = _multi_part_load_config_query(rel_name, file_type,
                                                _local_multipart_config_integration(raw_data_rel_name))

    return f"{_part_index_relation(part_indexes)}\n" \
           f"{raw_text}\n" \
           f"{load_config}\n" \
           f"{insert_text}"


def _remote_load_multipart_query(rel_name: str, file_type: FileType, parts, env_config: EnvConfig) -> str:
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
                                                _remote_multipart_config_integration(path_rel_name,
                                                                                     env_config))

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
module {_config_rel_name(rel_name)}[i in part_indexes]
    {schema}
    {config_integration}
end
"""


def _local_multipart_config_integration(raw_data_rel_name: str) -> str:
    return f"def data = {raw_data_rel_name}[i]"


def _remote_multipart_config_integration(path_rel_name: str, env_config: EnvConfig) -> str:
    return f"def integration:provider = \"azure\"\n" \
           f"def integration:credentials:azure_sas_token = raw\"{env_config.azure_import_sas}\"\n" \
           f"def path = {path_rel_name}[i]\n"


def _multi_part_insert_query(rel_name: str, file_type: FileType) -> str:
    conf_rel_name = _config_rel_name(rel_name)

    return f"def insert:source_catalog:{rel_name}[i] = {FILE_LOAD_RELATION[file_type]}[{conf_rel_name}[i]]"


def _simple_insert_query(rel_name: str, file_type: FileType) -> str:
    return f"def insert:simple_source_catalog:{rel_name} = {FILE_LOAD_RELATION[file_type]}[{IMPORT_CONFIG_REL}:{rel_name}]"


def _load_from_indexed_literal(data: str, raw_data_rel_name: str, index: int) -> str:
    return f"def {raw_data_rel_name}[{index}] =\n" \
           f"raw\"\"\"{data}" \
           f"\"\"\""


def _load_from_literal(data: str, raw_data_rel_name: str) -> str:
    return f"def {raw_data_rel_name} =\n" \
           f"raw\"\"\"{data}" \
           f"\"\"\""


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


def _export_relation_as_csv_remote(env_config: EnvConfig, export: Export, end_date: str, date_format: str) -> str:
    rel_name = export.relation
    export_path = f"{_compose_export_path(env_config, export, end_date, date_format)}/{rel_name}.csv"
    return f"""
    module _export_csv_config
        def {rel_name} = export_config:{rel_name}
        def {rel_name}:path = raw"{export_path}"
        def {rel_name} = _credentials_config
    end
    def export:{rel_name} = export_csv[_export_csv_config:{rel_name}]
    """


def _export_meta_relation_as_csv_remote(env_config: EnvConfig, export: Export, end_date: str, date_format: str) -> str:
    rel_name = export.relation
    postfix = _to_rel_meta_key_as_str(export)
    base_path = _compose_export_path(env_config, export, end_date, date_format)
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


def _compose_export_path(env_config: EnvConfig, export: Export, end_date: str, date_format: str) -> str:
    account_url = f"azure://{env_config.azure_export_account}.blob.core.windows.net"
    container_path = env_config.azure_export_container
    folder_path = env_config.azure_export_data_path
    date_path = utils.get_date_path(end_date, date_format, export.offset_by_number_of_days)
    if export.meta_key:
        postfix = _to_rel_meta_key_as_str(export)
        return f"{account_url}/{container_path}/{folder_path}/{export.relative_path}_{postfix}/{date_path}"
    else:
        return f"{account_url}/{container_path}/{folder_path}/{export.relative_path}/{date_path}"


def _to_rel_literal_relation(xs: Iterable[str]) -> str:
    return "{ " + " ; ".join([f"\"{x}\"" for x in xs]) + " }"


def _to_rel_meta_key_as_seq(export: Export) -> str:
    key_values = range(0, len(export.meta_key))
    return ", ".join([f"_v{i}" for i in key_values])


def _to_rel_meta_key_as_str(export: Export) -> str:
    key_values = range(0, len(export.meta_key))
    return "_".join([f"%(_v{i})" for i in key_values])
