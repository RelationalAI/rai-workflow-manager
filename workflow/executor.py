import dataclasses
import logging
import subprocess
import time
import asyncio
import concurrent.futures
from workflow import snow
from datetime import datetime
from enum import Enum
from itertools import groupby
from types import MappingProxyType
from typing import List

from more_itertools import peekable

from workflow import query as q, paths, rai, constants
from workflow.exception import StepTimeOutException, CommandExecutionException
from workflow.common import EnvConfig, RaiConfig, Source, BatchConfig, Export, FileType, ContainerType, Container, \
    FileMetadata
from workflow.manager import ResourceManager
from workflow.utils import save_csv_output, format_duration, build_models, extract_date_range, build_relation_path, \
    get_common_model_relative_path, get_or_create_eventloop


class WorkflowStepState(str, Enum):
    INIT = 'INIT'
    IN_PROGRESS = 'IN_PROGRESS'
    SUCCESS = 'SUCCESS'
    FAILED = 'FAILED'


@dataclasses.dataclass
class WorkflowConfig:
    env: EnvConfig
    batch_config: BatchConfig
    recover: bool
    recover_step: str
    selected_steps: List[str]
    step_params: dict
    step_timeout: dict[str, int] = None


class WorkflowStep:
    idt: str
    name: str
    type: str
    state: WorkflowStepState
    timing: int
    engine_size: str

    def __init__(self, idt: str, name: str, type_value: str, state: WorkflowStepState, timing: int, engine_size: str):
        self.idt = idt
        self.name = name
        self.type = type_value
        self.state = state
        self.timing = timing
        self.engine_size = engine_size

    def execute(self, logger: logging.Logger, env_config: EnvConfig, rai_config: RaiConfig):
        logger.info(f"Executing {self.get_name()} step...")
        logger = logger.getChild(self.name)
        self._execute(logger, env_config, rai_config)

    def get_name(self) -> str:
        return f"{self.name}({self.type})"

    def _execute(self, logger: logging.Logger, env_config: EnvConfig, rai_config: RaiConfig):
        raise NotImplementedError("This class is abstract")


class WorkflowStepFactory:

    def get_step(self, logger: logging.Logger, config: WorkflowConfig, step: dict) -> WorkflowStep:
        idt = step["idt"]
        name = step["name"]
        type_value = step["type"]
        engine_size = str.upper(step["engineSize"]) if "engineSize" in step else None
        state = WorkflowStepState(step.get("state"))
        timing = step.get("executionTime", 0)

        self._validate_params(config, step)
        return self._get_step(logger, config, idt, name, type_value, state, timing, engine_size, step)

    def _get_step(self, logger: logging.Logger, config: WorkflowConfig, idt, name, type_value, state, timing,
                  engine_size, step: dict) -> WorkflowStep:
        raise NotImplementedError("This class is abstract")

    def _required_params(self, config: WorkflowConfig) -> List[str]:
        return []

    def _validate_params(self, config: WorkflowConfig, step: dict) -> None:
        params = self._required_params(config)
        missed_params = []
        for param in params:
            if param not in config.step_params:
                missed_params.append(param)
        if missed_params:
            raise ValueError(f"Workflow config doesn't have required parameters for {step['name']} : '{missed_params}'")


class InstallModelsStep(WorkflowStep):
    rel_config_dir: str
    model_files: List[str]

    def __init__(self, idt, name, type_value, state, timing, engine_size, rel_config_dir, model_files):
        super().__init__(idt, name, type_value, state, timing, engine_size)
        self.rel_config_dir = rel_config_dir
        self.model_files = model_files

    def _execute(self, logger: logging.Logger, env_config: EnvConfig, rai_config: RaiConfig):
        rai.install_models(logger, rai_config, env_config, build_models(self.model_files, self.rel_config_dir))


class InstallModelWorkflowStepFactory(WorkflowStepFactory):

    def _required_params(self, config: WorkflowConfig) -> List[str]:
        return [constants.REL_CONFIG_DIR]

    def _get_step(self, logger: logging.Logger, config: WorkflowConfig, idt, name, type_value, state, timing,
                  engine_size, step: dict) -> WorkflowStep:
        rel_config_dir = config.step_params[constants.REL_CONFIG_DIR]
        return InstallModelsStep(idt, name, type_value, state, timing, engine_size, rel_config_dir, step["modelFiles"])


class ConfigureSourcesWorkflowStep(WorkflowStep):
    config_files: List[str]
    rel_config_dir: str
    sources: List[Source]
    paths_builders: dict[str, paths.PathsBuilder]
    start_date: str
    end_date: str
    force_reimport: bool
    force_reimport_not_chunk_partitioned: bool

    def __init__(self, idt, name, type_value, state, timing, engine_size, config_files, rel_config_dir, sources,
                 paths_builders, start_date, end_date, force_reimport, force_reimport_not_chunk_partitioned):
        super().__init__(idt, name, type_value, state, timing, engine_size)
        self.config_files = config_files
        self.rel_config_dir = rel_config_dir
        self.sources = sources
        self.paths_builders = paths_builders
        self.start_date = start_date
        self.end_date = end_date
        self.force_reimport = force_reimport
        self.force_reimport_not_chunk_partitioned = force_reimport_not_chunk_partitioned

    def _execute(self, logger: logging.Logger, env_config: EnvConfig, rai_config: RaiConfig):
        rai.install_models(logger, rai_config, env_config, build_models(self.config_files, self.rel_config_dir))

        self._inflate_sources(logger, rai_config, env_config)
        # calculate expired sources
        declared_sources = {src["source"]: src for src in
                            rai.execute_relation_json(logger, rai_config, env_config,
                                                      constants.DECLARED_DATE_PARTITIONED_SOURCE_REL)}
        expired_sources = self._calculate_expired_sources(logger, declared_sources)

        # mark declared sources for reimport
        rai.execute_query(logger, rai_config, env_config,
                          q.discover_reimport_sources(self.sources, expired_sources, self.force_reimport,
                                                      self.force_reimport_not_chunk_partitioned),
                          readonly=False)
        # populate declared sources
        rai.execute_query(logger, rai_config, env_config, q.populate_source_configs(self.sources), readonly=False)

    def _inflate_sources(self, logger: logging.Logger, rai_config: RaiConfig, env_config: EnvConfig):
        for src in self.sources:
            logger.info(f"Inflating source: '{src.relation}'")
            days = self._get_date_range(logger, src)
            if src.snapshot_validity_days and src.snapshot_validity_days > 0:
                query = q.get_snapshot_expiration_date(src.relation, constants.DATE_FORMAT)
                expiration_date_str = rai.execute_query_take_single(logger, rai_config, env_config, query)
                if expiration_date_str:
                    current_date = datetime.strptime(self.end_date, constants.DATE_FORMAT)
                    expiration_date = datetime.strptime(expiration_date_str, constants.DATE_FORMAT)
                    if expiration_date >= current_date:
                        logger.info(f"Snapshot source '{src.relation}' within validity days. Skipping inflate paths...")
                        continue

            inflated_paths = self.paths_builders[src.container.name].build(logger, days, src.relative_path,
                                                                           src.extensions,
                                                                           src.is_date_partitioned)
            if src.is_size_supported():
                self.__print_total_size(logger, inflated_paths)
            if src.is_date_partitioned:
                # after inflating we take the last `src.loads_number_of_days` days and reduce into an array of paths
                grouped_inflated_paths = ConfigureSourcesWorkflowStep.__group_paths_by_date(inflated_paths)
                date_path_tuples = list(grouped_inflated_paths.items())
                # Take the last `src.loads_number_of_days` tuples
                last_date_paths_tuples = date_path_tuples[-src.loads_number_of_days:]
                inflated_paths = [path for date, date_paths in last_date_paths_tuples for path in date_paths]

            if not src.is_chunk_partitioned:
                grouped_inflated_paths = ConfigureSourcesWorkflowStep.__group_paths_by_date(inflated_paths)
                inflated_paths = []
                # Take only one (the first) file from each not chunk partitioned source
                for date, date_paths in grouped_inflated_paths.items():
                    if len(date_paths) > 1:
                        elements = "\n".join([f"{obj.path}" for obj in date_paths])
                        logger.warning(
                            f"Source '{src.relation}' is not chunk partitioned, but has more than one file:\n"
                            f"{elements}.\nTaking only the first one: {date_paths[0]}")
                    inflated_paths.append(date_paths[0])

            src.paths = [p.path for p in inflated_paths]

    def _get_date_range(self, logger, src):
        days = []
        # For snapshot sources we restrict the `loads_number_of_days` to the `snapshot_validity_days` minus
        # `offset_by_number_of_days` if the former is set, so we take a full data range to seek the latest
        # snapshot within. Otherwise, we use the `loads_number_of_days` as is.
        # Note: `offset_by_number_of_days` must be less than or equal to `snapshot_validity_days`.
        if src.is_date_partitioned:
            offset_by_number_of_days = src.offset_by_number_of_days if src.offset_by_number_of_days else 0
            loads_number_of_days = (src.snapshot_validity_days - offset_by_number_of_days + 1) \
                if src.snapshot_validity_days else src.loads_number_of_days
            if loads_number_of_days < 0:
                raise ValueError(f"Values must be: `offset_by_number_of_days` <= `snapshot_validity_days`")
            days.extend(extract_date_range(logger, self.start_date, self.end_date, loads_number_of_days,
                                           offset_by_number_of_days))
        return days

    def _calculate_expired_sources(self, logger: logging.Logger, declared_sources: dict[str, dict]) -> \
            List[tuple[str, str]]:
        expired_resources = []
        for source in self.sources:
            if source.relation in declared_sources:
                declared_source = declared_sources[source.relation]
                source_days = self._get_date_range(logger, source)
                for date_res in declared_source["dates"]:
                    if date_res["date"] not in source_days:
                        for p in date_res["paths"]:
                            expired_resources.append((source.relation, p))
        return expired_resources

    @staticmethod
    def __group_paths_by_date(src_paths) -> dict[str, List[FileMetadata]]:
        src_paths.sort(key=lambda v: v.as_of_date)
        return {date: list(group) for date, group in
                groupby(src_paths, key=lambda v: v.as_of_date)}

    @staticmethod
    def __print_total_size(logger, inflated_paths):
        total_size = sum([path.size if path.size else 0 for path in inflated_paths])

        # Determine the appropriate unit (bytes, KB, MB, GB, TB)
        size_units = ['bytes', 'KB', 'MB', 'GB', 'TB']
        size_unit_index = 0
        while total_size > 1024 and size_unit_index < len(size_units) - 1:
            total_size /= 1024.0
            size_unit_index += 1

        logger.info(f"Total size: {total_size:.2f} {size_units[size_unit_index]}")


class ConfigureSourcesWorkflowStepFactory(WorkflowStepFactory):

    def _validate_params(self, config: WorkflowConfig, step: dict) -> None:
        super()._validate_params(config, step)
        end_date = config.step_params[constants.END_DATE]
        sources = self._parse_sources(step, config.env)
        for s in sources:
            if s.loads_number_of_days and s.snapshot_validity_days and \
                    s.loads_number_of_days > 1 and s.snapshot_validity_days > 0:
                raise ValueError(f"No support more than 1 `loadNumberOfDays` for snapshot source: {s.relation}")
            if s.loads_number_of_days and s.snapshot_validity_days and \
                    s.loads_number_of_days > s.snapshot_validity_days > 0:
                raise ValueError(
                    f"`snapshotValidityDays` should be less or equal to `loadNumberOfDays`. Source: {s.relation}")
            if s.is_date_partitioned:
                if not end_date:
                    raise ValueError(f"End date is required for date partitioned source: {s.relation}")
                if not s.loads_number_of_days:
                    raise ValueError(f"`loadNumberOfDays` is required for date partitioned source: {s.relation}")

    def _required_params(self, config: WorkflowConfig) -> List[str]:
        return [constants.REL_CONFIG_DIR, constants.START_DATE, constants.END_DATE]

    def _get_step(self, logger: logging.Logger, config: WorkflowConfig, idt, name, type_value, state, timing,
                  engine_size, step: dict) -> ConfigureSourcesWorkflowStep:
        rel_config_dir = config.step_params[constants.REL_CONFIG_DIR]
        sources = self._parse_sources(step, config.env)
        start_date = config.step_params[constants.START_DATE]
        end_date = config.step_params[constants.END_DATE]
        force_reimport = config.step_params.get(constants.FORCE_REIMPORT, False)
        force_reimport_not_chunk_partitioned = config.step_params.get(constants.FORCE_REIMPORT_NOT_CHUNK_PARTITIONED,
                                                                      False)
        paths_builders = {}
        for src in sources:
            container = src.container
            if container.name not in paths_builders:
                paths_builders[container.name] = paths.PathsBuilderFactory.get_path_builder(container)
        return ConfigureSourcesWorkflowStep(idt, name, type_value, state, timing, engine_size, step["configFiles"],
                                            rel_config_dir, sources, paths_builders, start_date, end_date,
                                            force_reimport, force_reimport_not_chunk_partitioned)

    @staticmethod
    def _parse_sources(step: dict, env_config: EnvConfig) -> List[Source]:
        sources = step["sources"]
        default_container = step["defaultContainer"]
        result = []
        for source in sources:
            if "future" not in source or not source["future"]:
                relation = source["relation"]
                relative_path = source["relativePath"]
                input_format = source["inputFormat"]
                extensions = source.get("extensions", [input_format])
                is_chunk_partitioned = source.get("isChunkPartitioned", False)
                is_date_partitioned = source.get("isDatePartitioned", False)
                loads_number_of_days = source.get("loadsNumberOfDays")
                offset_by_number_of_days = source.get("offsetByNumberOfDays")
                snapshot_validity_days = source.get("snapshotValidityDays")
                container_name = source.get("container", default_container)
                result.append(Source(
                    env_config.get_container(container_name),
                    relation,
                    relative_path,
                    input_format,
                    extensions,
                    is_chunk_partitioned,
                    is_date_partitioned,
                    loads_number_of_days,
                    offset_by_number_of_days,
                    snapshot_validity_days
                ))
        return result


class LoadDataWorkflowStep(WorkflowStep):
    collapse_partitions_on_load: bool

    def __init__(self, idt, name, type_value, state, timing, engine_size, collapse_partitions_on_load):
        super().__init__(idt, name, type_value, state, timing, engine_size)
        self.collapse_partitions_on_load = collapse_partitions_on_load

    def _execute(self, logger: logging.Logger, env_config: EnvConfig, rai_config: RaiConfig):
        rai.execute_query(logger, rai_config, env_config, q.DELETE_REFRESHED_SOURCES_DATA, readonly=False)

        missed_resources = rai.execute_relation_json(logger, rai_config, env_config, constants.MISSED_RESOURCES_REL)

        if not missed_resources:
            logger.info("Missed resources list is empty")

        # separate async and simple resources to avoid parallel writes to the same database by 2 different engines
        async_resources = []
        simple_resources = []
        for src in missed_resources:
            if self._resource_is_async(src):
                async_resources.append(src)
            else:
                simple_resources.append(src)

        for src in simple_resources:
            self._load_source(logger, env_config, rai_config, src)

        if async_resources:
            for src in async_resources:
                self._load_source(logger, env_config, rai_config, src)
            self.await_pending(env_config, logger, missed_resources)

    def await_pending(self, env_config, logger, missed_resources):
        loop = get_or_create_eventloop()
        if loop.is_running():
            raise Exception('Waiting for resource would interrupt unexpected event loop - aborting to avoid confusion')
        pending = [src for src in missed_resources if self._resource_is_async(src)]
        pending_cos = [self._await_async_resource(logger, env_config, resource) for resource in pending]
        loop.run_until_complete(asyncio.gather(*pending_cos))

    async def _await_async_resource(self, logger: logging.Logger, env_config: EnvConfig, src):
        container = env_config.get_container(src["container"])
        config = EnvConfig.get_config(container)
        if ContainerType.SNOWFLAKE == container.type:
            await snow.await_data_sync(logger, config, src["resources"])

    def _load_source(self, logger: logging.Logger, env_config: EnvConfig, rai_config: RaiConfig, src):
        source_name = src["source"]
        if 'is_date_partitioned' in src and src['is_date_partitioned'] == 'Y':
            logger.info(f"Loading source '{source_name}' partitioned by date")
            if self.collapse_partitions_on_load:
                srcs = src["dates"]
                first_date = srcs[0]["date"]
                last_date = srcs[-1]["date"]

                logger.info(
                    f"Loading '{source_name}' all date partitions simultaneously, range {first_date} to {last_date}")

                resources = []
                for d in srcs:
                    resources += d["resources"]
                self._load_resource(logger, env_config, rai_config, resources, src)
            else:
                logger.info(f"Loading '{source_name}' one date partition at a time")
                for d in src["dates"]:
                    logger.info(f"Loading partition for date {d['date']}")

                    for res in d["resources"]:
                        self._load_resource(logger, env_config, rai_config, [res], src)
        else:
            logger.info(f"Loading source '{source_name}' not partitioned by date")
            if self.collapse_partitions_on_load:
                logger.info(f"Loading '{source_name}' all chunk partitions simultaneously")
                self._load_resource(logger, env_config, rai_config, src["resources"], src)
            else:
                logger.info(f"Loading '{source_name}' one chunk partition at a time")
                for res in src["resources"]:
                    self._load_resource(logger, env_config, rai_config, [res], src)

    @staticmethod
    def _resource_is_async(src):
        return True if ContainerType.SNOWFLAKE == ContainerType.from_source(src) else False

    @staticmethod
    def _load_resource(logger: logging.Logger, env_config: EnvConfig, rai_config: RaiConfig, resources, src) -> None:
        try:
            container = env_config.get_container(src["container"])
            config = EnvConfig.get_config(container)
            if ContainerType.LOCAL == container.type or ContainerType.AZURE == container.type:
                query_with_input = q.load_resources(logger, config, resources, src)
                rai.execute_query(logger, rai_config, env_config, query_with_input.query, query_with_input.inputs,
                                  readonly=False)
            elif ContainerType.SNOWFLAKE == container.type:
                snow.begin_data_sync(logger, config, rai_config, resources, src)
        except KeyError as e:
            logger.error(f"Unsupported file type: {src['file_type']}. Skip the source: {src}", e)
        except ValueError as e:
            logger.error(f"Unsupported source type. Skip the source: {src}", e)


class LoadDataWorkflowStepFactory(WorkflowStepFactory):

    def _required_params(self, config: WorkflowConfig) -> List[str]:
        return [constants.COLLAPSE_PARTITIONS_ON_LOAD]

    def _get_step(self, logger: logging.Logger, config: WorkflowConfig, idt, name, type_value, state, timing,
                  engine_size, step: dict) -> WorkflowStep:
        collapse_partitions_on_load = config.step_params[constants.COLLAPSE_PARTITIONS_ON_LOAD]
        return LoadDataWorkflowStep(idt, name, type_value, state, timing, engine_size, collapse_partitions_on_load)


class MaterializeWorkflowStep(WorkflowStep):
    relations: List[str]
    materialize_jointly: bool

    def __init__(self, idt, name, type_value, state, timing, engine_size, relations, materialize_jointly):
        super().__init__(idt, name, type_value, state, timing, engine_size)
        self.relations = relations
        self.materialize_jointly = materialize_jointly

    def _execute(self, logger: logging.Logger, env_config: EnvConfig, rai_config: RaiConfig):
        if self.materialize_jointly:
            rai.execute_query(logger, rai_config, env_config, q.materialize(self.relations), readonly=False)
        else:
            for relation in self.relations:
                rai.execute_query(logger, rai_config, env_config, q.materialize([relation]), readonly=False)


class MaterializeWorkflowStepFactory(WorkflowStepFactory):

    def _get_step(self, logger: logging.Logger, config: WorkflowConfig, idt, name, type_value, state, timing,
                  engine_size, step: dict) -> WorkflowStep:
        return MaterializeWorkflowStep(idt, name, type_value, state, timing, engine_size, step["relations"],
                                       step["materializeJointly"])


class ExportWorkflowStep(WorkflowStep):
    exports: List[Export]
    export_jointly: bool
    date_format: str
    end_date: str

    EXPORT_FUNCTION = {
        ContainerType.LOCAL:
            lambda logger, rai_config, env_config, exports, end_date, date_format, container: save_csv_output(
                rai.execute_query_csv(logger, rai_config, env_config, q.export_relations_local(logger, exports)),
                EnvConfig.get_config(container)),
        ContainerType.AZURE:
            lambda logger, rai_config, env_config, exports, end_date, date_format, container: rai.execute_query(
                logger, rai_config, env_config,
                q.export_relations_to_azure(logger, EnvConfig.get_config(container), exports, end_date, date_format))
    }

    def __init__(self, idt, name, type_value, state, timing, engine_size, exports, export_jointly, date_format,
                 end_date):
        super().__init__(idt, name, type_value, state, timing, engine_size)
        self.exports = exports
        self.export_jointly = export_jointly
        self.date_format = date_format
        self.end_date = end_date

    def _execute(self, logger: logging.Logger, env_config: EnvConfig, rai_config: RaiConfig):
        exports = list(filter(lambda e: self._should_export(logger, rai_config, env_config, e), self.exports))
        if self.export_jointly:
            exports.sort(key=lambda e: e.container.name)
            container_groups = {container_name: list(group) for container_name, group in
                                groupby(exports, key=lambda e: e.container.name)}
            for container_name, grouped_exports in container_groups.items():
                container = env_config.get_container(container_name)
                ExportWorkflowStep.get_export_function(container)(logger, rai_config, env_config, grouped_exports,
                                                                  self.end_date, self.date_format, container)
        else:
            for export in exports:
                container = export.container
                ExportWorkflowStep.get_export_function(container)(logger, rai_config, env_config, [export],
                                                                  self.end_date, self.date_format, container)

    @staticmethod
    def get_export_function(container: Container):
        try:
            return ExportWorkflowStep.EXPORT_FUNCTION[container.type]
        except KeyError as ex:
            raise ValueError(f"Container type is not supported: {ex}")

    def _should_export(self, logger: logging.Logger, rai_config: RaiConfig, env_config: EnvConfig,
                       export: Export) -> bool:
        if export.snapshot_binding is None:
            return True
        logger.info(f"Checking validity of snapshot: {export.snapshot_binding}")
        current_date = datetime.strptime(self.end_date, self.date_format)
        query = q.get_snapshot_expiration_date(export.snapshot_binding, self.date_format)
        expiration_date_str = rai.execute_query_take_single(logger, rai_config, env_config, query)
        # if nothing returned we opt for exporting the snapshot
        if expiration_date_str is None:
            return True
        expiration_date = datetime.strptime(expiration_date_str, self.date_format)
        should_export = expiration_date <= current_date
        if not should_export:
            logger.info(
                f"Skipping export of {export.relation}: defined as a snapshot and the current one is still valid")
        return should_export


class ExportWorkflowStepFactory(WorkflowStepFactory):

    def _required_params(self, config: WorkflowConfig) -> List[str]:
        return [constants.END_DATE]

    def _get_step(self, logger: logging.Logger, config: WorkflowConfig, idt, name, type_value, state, timing,
                  engine_size, step: dict) -> WorkflowStep:
        exports = self._load_exports(logger, config.env, step)
        end_date = config.step_params[constants.END_DATE]
        return ExportWorkflowStep(idt, name, type_value, state, timing, engine_size, exports, step["exportJointly"],
                                  step["dateFormat"], end_date)

    @staticmethod
    def _load_exports(logger: logging.Logger, env_config: EnvConfig, src) -> List[Export]:
        exports_json = src["exports"]
        default_container = src["defaultContainer"]
        exports = []
        for e in exports_json:
            if "future" not in e or not e["future"]:
                try:
                    exports.append(Export(meta_key=e.get("metaKey", []),
                                          relation=e["configRelName"],
                                          relative_path=e["relativePath"],
                                          file_type=FileType[e["type"].upper()],
                                          snapshot_binding=e.get("snapshotBinding"),
                                          container=env_config.get_container(e.get("container", default_container)),
                                          offset_by_number_of_days=e.get("offsetByNumberOfDays", 0)))
                except KeyError as ex:
                    logger.warning(f"Unsupported FileType: {ex}. Skipping export: {e}")
        return exports


class ExecuteCommandWorkflowStep(WorkflowStep):
    command: str

    def __init__(self, idt, name, type_value, state, timing, engine_size, command):
        super().__init__(idt, name, type_value, state, timing, engine_size)
        self.command = command

    def _execute(self, logger: logging.Logger, env_config: EnvConfig, rai_config: RaiConfig):
        process = subprocess.Popen(self.command, shell=True, text=True)
        exit_code = process.wait()
        if exit_code != 0:
            raise CommandExecutionException(self.command, process.returncode)


class ExecuteCommandWorkflowStepFactory(WorkflowStepFactory):

    def _get_step(self, logger: logging.Logger, config: WorkflowConfig, idt, name, type_value, state, timing,
                  engine_size, step: dict) -> WorkflowStep:
        return ExecuteCommandWorkflowStep(idt, name, type_value, state, timing, engine_size, step["command"])


DEFAULT_FACTORIES = MappingProxyType(
    {
        constants.CONFIGURE_SOURCES: ConfigureSourcesWorkflowStepFactory(),
        constants.INSTALL_MODELS: InstallModelWorkflowStepFactory(),
        constants.LOAD_DATA: LoadDataWorkflowStepFactory(),
        constants.MATERIALIZE: MaterializeWorkflowStepFactory(),
        constants.EXPORT: ExportWorkflowStepFactory(),
        constants.EXECUTE_COMMAND: ExecuteCommandWorkflowStepFactory()
    }
)


class WorkflowExecutor:
    logger: logging.Logger
    config: WorkflowConfig
    resource_manager: ResourceManager
    steps: List[WorkflowStep]

    def __init__(self, logger: logging.Logger, config: WorkflowConfig, resource_manager: ResourceManager, steps):
        self.logger = logger
        self.config = config
        self.resource_manager = resource_manager
        self.steps = steps

    def run(self):
        recover_step_reached = False
        rai_config = self.resource_manager.get_rai_config()
        steps_iter = peekable(self.steps)
        for step in steps_iter:
            if self.config.selected_steps:
                if step.name not in self.config.selected_steps:
                    self.logger.info(f"Step {step.name} (id='{step.idt}') is not selected. Skipping...")
                    continue
            else:
                # `recover_step` option has higher priority than `recover` option
                if self.config.recover_step and not recover_step_reached:
                    if step.name == self.config.recover_step:
                        recover_step_reached = True
                    if not recover_step_reached:
                        self.logger.info(
                            f"Recovery... Skipping the step {step.name} (id='{step.idt}') till reach recovery step")
                        continue
                elif self.config.recover and step.state == WorkflowStepState.SUCCESS:
                    self.logger.info(f"Recovery... Skipping the successful step {step.name} (id='{step.idt}')")
                    continue

            start_time = time.time()
            self._update_step_state(step, rai_config, WorkflowStepState.IN_PROGRESS)
            try:
                if step.engine_size:
                    self.resource_manager.add_engine(step.engine_size)
                    self.execute_step(step, self.resource_manager.get_rai_config(step.engine_size))
                    next_step = steps_iter.peek(None)
                    if next_step and next_step.engine_size != step.engine_size:
                        self.resource_manager.remove_engine(step.engine_size)
                else:
                    self.execute_step(step, rai_config)

                end_time = time.time()
                execution_time = end_time - start_time
                query = "\n".join([q.update_step_state(step.idt, WorkflowStepState.SUCCESS.name),
                                   q.update_execution_time(step.idt, execution_time)])
                self.logger.info(f"Update {step.get_name()}'s timings and state to '{WorkflowStepState.SUCCESS.name}'")
                rai.execute_query(self.logger, rai_config, self.config.env, query, readonly=False)
            except StepTimeOutException as e:
                # Skip step state update since write txn can be stuck and RWM will raise ConcurrentWriteAttemptException
                if step.engine_size:
                    self.resource_manager.remove_engine(step.engine_size)
                raise e
            except Exception as e:
                self._update_step_state(step, rai_config, WorkflowStepState.FAILED)
                if step.engine_size:
                    self.resource_manager.remove_engine(step.engine_size)
                raise e

            self.logger.info(f"{step.get_name()} finished in {format_duration(execution_time)}")

    def _update_step_state(self, step: WorkflowStep, rai_config: RaiConfig, state: WorkflowStepState) -> None:
        self.logger.info(f"Update {step.get_name()}'s state to '{state.name}'")
        rai.execute_query(self.logger, rai_config, self.config.env, q.update_step_state(step.idt, state.name),
                          readonly=False, ignore_problems=True)

    def print_timings(self) -> None:
        rai_config = self.resource_manager.get_rai_config()
        env_config = self.config.env
        relation = build_relation_path(constants.WORKFLOW_JSON_REL, self.config.batch_config.name)
        workflow_info = rai.execute_relation_json(self.logger, rai_config, env_config, relation, ignore_problems=True)
        steps = workflow_info["steps"]
        for step in steps:
            execution_time = step["executionTime"]
            self.logger.info(f"{step['name']}({step['type']}) finished in {format_duration(execution_time)}")
        type_groups = {step_type: list(group) for step_type, group in
                       groupby(steps, key=lambda e: e['type'])}
        for step_type, group_steps in type_groups.items():
            if len(group_steps) > 1:
                execution_time = sum([step["executionTime"] for step in group_steps])
                self.logger.info(f"{step_type} steps execution time is {format_duration(execution_time)}")
        self.logger.info(f"Total workflow execution time is {format_duration(workflow_info['totalTime'])}")

    def execute_step(self, step, rai_config: RaiConfig) -> None:
        if self.config.step_timeout and self.config.step_timeout.get(step.name, 0) > 0:
            timeout = self.config.step_timeout.get(step.name)
            with concurrent.futures.ThreadPoolExecutor() as executor:
                # Submit the function to the executor
                future = executor.submit(step.execute, self.logger, self.config.env, rai_config)
                try:
                    # Wait for the function to complete, with a maximum timeout in WorkflowConfig for the step
                    future.result(timeout=timeout)
                except concurrent.futures.TimeoutError:
                    raise StepTimeOutException(f"Step '{step.name}' exceeded step's timeout: {timeout} sec")
        else:
            step.execute(self.logger, self.config.env, rai_config)

    @staticmethod
    def init(logger: logging.Logger, config: WorkflowConfig, resource_manager: ResourceManager,
             factories: dict[str, WorkflowStepFactory] = MappingProxyType({}),
             models: dict[str, str] = MappingProxyType({})):
        logger = logger.getChild("workflow")
        rai_config = resource_manager.get_rai_config()

        if not config.recover and not config.recover_step:
            # Install common model for workflow manager
            core_models = build_models(constants.COMMON_MODEL, get_common_model_relative_path(__file__))
            extended_models = {**core_models, **models}
            logger.info("Installing RWM common models...")
            rai.install_models(logger, rai_config, config.env, extended_models)

        # Load batch config
        batch_config_relation = build_relation_path(constants.CONFIG_BASE_RELATION, config.batch_config.name)
        logger.info(f"Cleanup batch config `{config.batch_config.name}`")
        rai.execute_query(logger, rai_config, config.env, q.delete_relation(batch_config_relation), readonly=False)
        logger.info(f"Load a new batch config `{config.batch_config.name}`")
        rai.load_json(logger, rai_config, config.env, batch_config_relation, config.batch_config.content)

        if not config.recover and not config.recover_step:
            # Init workflow steps
            logger.info("Init workflow steps")
            rai.execute_query(logger, rai_config, config.env, q.init_workflow_steps(config.batch_config.name),
                              readonly=False)

        extended_factories = {**DEFAULT_FACTORIES, **factories}
        workflow_info = rai.execute_relation_json(logger, rai_config, config.env,
                                                  build_relation_path(constants.WORKFLOW_JSON_REL,
                                                                      config.batch_config.name), ignore_problems=True)
        steps_json = workflow_info["steps"]
        if not steps_json:
            raise ValueError(f"Config `{config.batch_config.name}` doesn't have workflow steps")
        steps = []
        for step in steps_json:
            step_type = step["type"]
            if step_type not in extended_factories:
                logger.warning(f"Step '{step_type}' is not supported")
            else:
                steps.append(extended_factories.get(step_type).get_step(logger, config, step))
        return WorkflowExecutor(logger, config, resource_manager, steps)
