import logging
import time
import dataclasses
from more_itertools import peekable
from typing import List, Dict
from enum import Enum

from workflow.utils import save_csv_output, format_duration, build_models, extract_date_range, build_relation_path, \
    get_common_model_relative_path
from workflow.common import EnvConfig, RaiConfig, Source, BatchConfig, Export, FileType
from workflow.manager import ResourceManager
from workflow import query as q, paths, rai, constants
from types import MappingProxyType

CONFIGURE_SOURCES = 'ConfigureSources'
INSTALL_MODELS = 'InstallModels'
LOAD_DATA = 'LoadData'
# not supported
INVOKE_SOLVER = 'InvokeSolver'
MATERIALIZE = 'Materialize'
EXPORT = 'Export'


class WorkflowRunMode(Enum):
    LOCAL = 'local'
    REMOTE = 'remote'

    def __str__(self):
        return self.value


class WorkflowStepState(str, Enum):
    INIT = 'INIT'
    IN_PROGRESS = 'IN_PROGRESS'
    SUCCESS = 'SUCCESS'
    FAILED = 'FAILED'


@dataclasses.dataclass
class WorkflowConfig:
    env: EnvConfig
    run_mode: WorkflowRunMode
    batch_config: BatchConfig
    recover: bool
    recover_step: str
    selected_steps: List[str]
    step_params: dict


class WorkflowStep:
    idt: str
    name: str
    state: WorkflowStepState
    timing: int
    engine_size: str

    def __init__(self, idt, name, state, timing, engine_size):
        self.idt = idt
        self.name = name
        self.state = state
        self.time = timing
        self.engine_size = engine_size

    def execute(self, logger: logging.Logger, env_config: EnvConfig, rai_config: RaiConfig):
        raise NotImplementedError("This class is abstract")


class WorkflowStepFactory:

    def get_step(self, logger: logging.Logger, config: WorkflowConfig, step: dict) -> WorkflowStep:
        idt = step["idt"]
        name = step["name"]
        engine_size = str.upper(step["engineSize"]) if "engineSize" in step else None
        state = WorkflowStepState(step.get("state"))
        timing = step.get("executionTime", 0)

        self._validate_params(config, step)
        return self._get_step(logger, config, idt, name, state, timing, engine_size, step)

    def _get_step(self, logger: logging.Logger, config: WorkflowConfig, idt, name, state, timing, engine_size,
                  step: dict) -> WorkflowStep:
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

    def __init__(self, idt, name, state, timing, engine_size, rel_config_dir, model_files):
        super().__init__(idt, name, state, timing, engine_size)
        self.rel_config_dir = rel_config_dir
        self.model_files = model_files

    def execute(self, logger: logging.Logger, env_config: EnvConfig, rai_config: RaiConfig):
        logger.info("Executing InstallModel step..")

        rai.install_models(logger, rai_config, build_models(self.model_files, self.rel_config_dir))


class InstallModelWorkflowStepFactory(WorkflowStepFactory):

    def _required_params(self, config: WorkflowConfig) -> List[str]:
        return [constants.REL_CONFIG_DIR]

    def _get_step(self, logger: logging.Logger, config: WorkflowConfig, idt, name, state, timing, engine_size,
                  step: dict) -> WorkflowStep:
        rel_config_dir = config.step_params[constants.REL_CONFIG_DIR]
        return InstallModelsStep(idt, name, state, timing, engine_size, rel_config_dir, step["modelFiles"])


class ConfigureSourcesWorkflowStep(WorkflowStep):
    config_files: List[str]
    rel_config_dir: str
    sources: List[Source]
    paths_builder: paths.PathsBuilder
    start_date: str
    end_date: str

    def __init__(self, idt, name, state, timing, engine_size, config_files, rel_config_dir, sources, paths_builder,
                 start_date, end_date):
        super().__init__(idt, name, state, timing, engine_size)
        self.config_files = config_files
        self.rel_config_dir = rel_config_dir
        self.sources = sources
        self.paths_builder = paths_builder
        self.start_date = start_date
        self.end_date = end_date

    def execute(self, logger: logging.Logger, env_config: EnvConfig, rai_config: RaiConfig):
        logger.info("Executing ConfigureSources step..")

        rai.install_models(logger, rai_config, build_models(self.config_files, self.rel_config_dir))

        self._inflate_sources(logger)
        rai.execute_query(logger, rai_config, q.populate_source_configs(self.sources), readonly=False)

    def _inflate_sources(self, logger: logging.Logger):
        for src in self.sources:
            logger.info(f"Inflating source: '{src.relation}'")
            date_range = []
            if src.is_date_partitioned:
                date_range.extend(extract_date_range(logger, self.start_date, self.end_date, src.loads_number_of_days,
                                                     src.offset_by_number_of_days))

            inflated_paths = self.paths_builder.build(logger, date_range, src.relative_path, src.extensions,
                                                      src.is_date_partitioned)
            src.paths = inflated_paths


class ConfigureSourcesWorkflowStepFactory(WorkflowStepFactory):

    def _validate_params(self, config: WorkflowConfig, step: dict) -> None:
        super()._validate_params(config, step)
        end_date = config.step_params[constants.END_DATE]
        sources = self._parse_sources(step["sources"])
        if not end_date:
            for s in sources:
                if s.is_date_partitioned:
                    raise ValueError(f"End date is required for date partitioned source: {s.relation}")

    def _required_params(self, config: WorkflowConfig) -> List[str]:
        required_params = [constants.REL_CONFIG_DIR, constants.START_DATE, constants.END_DATE]
        if config.run_mode == WorkflowRunMode.LOCAL:
            required_params.append(constants.LOCAL_DATA_DIR)
        return required_params

    def _get_step(self, logger: logging.Logger, config: WorkflowConfig, idt, name, state, timing, engine_size,
                  step: dict) -> WorkflowStep:
        if config.run_mode == WorkflowRunMode.LOCAL:
            local_data_dir = config.step_params[constants.LOCAL_DATA_DIR]
            paths_builder = paths.LocalPathsBuilder(local_data_dir)
        elif config.run_mode == WorkflowRunMode.REMOTE:
            paths_builder = paths.RemotePathsBuilder(config.env)
        else:
            raise Exception("unsupported mode")
        rel_config_dir = config.step_params[constants.REL_CONFIG_DIR]
        sources = self._parse_sources(step["sources"])
        start_date = config.step_params[constants.START_DATE]
        end_date = config.step_params[constants.END_DATE]
        return ConfigureSourcesWorkflowStep(idt, name, state, timing, engine_size, step["configFiles"], rel_config_dir,
                                            sources, paths_builder, start_date, end_date)

    @staticmethod
    def _parse_sources(sources: List[Dict]) -> List[Source]:
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
                result.append(Source(
                    relation,
                    relative_path,
                    input_format,
                    extensions,
                    is_chunk_partitioned,
                    is_date_partitioned,
                    loads_number_of_days,
                    offset_by_number_of_days,
                    []
                ))
        return result


class LoadDataWorkflowStep(WorkflowStep):
    collapse_partitions_on_load: bool

    def __init__(self, idt, name, state, timing, engine_size, collapse_partitions_on_load):
        super().__init__(idt, name, state, timing, engine_size)
        self.collapse_partitions_on_load = collapse_partitions_on_load

    def execute(self, logger: logging.Logger, env_config: EnvConfig, rai_config: RaiConfig):
        logger.info("Executing LoadData step..")

        missed_resources = rai.execute_relation_json(logger, rai_config, constants.MISSED_RESOURCES_REL)

        if not missed_resources:
            logger.info("Missed resources list is empty")

        for src in missed_resources:
            self._load_source(logger, env_config, rai_config, src)

    def _load_source(self, logger: logging.Logger, env_config: EnvConfig, rai_config: RaiConfig, src):
        source_name = src["source"]
        if 'is_date_partitioned' in src and src['is_date_partitioned'] == 'Y':
            if self.collapse_partitions_on_load:
                srcs = src["dates"]
                first_date = srcs[0]["date"]
                last_date = srcs[-1]["date"]

                logger.info(
                    f"Loading '{source_name}' from all partitions simultaneously, range {first_date} to {last_date}")

                resources = []
                for d in srcs:
                    resources += d["resources"]
                self._load_resource(logger, env_config, rai_config, resources, src)
            else:
                logger.info(f"Loading '{source_name}' one partition at a time")
                for d in src["dates"]:
                    logger.info(f"Loading partition for date {d['date']}")

                    resources = d["resources"]
                    self._load_resource(logger, env_config, rai_config, resources, src)
        else:
            logger.info(f"Loading master source'{source_name}'")
            self._load_resource(logger, env_config, rai_config, src["resources"], src)

    @staticmethod
    def _load_resource(logger: logging.Logger, env_config: EnvConfig, rai_config: RaiConfig, resources, src) -> None:
        try:
            rai.execute_query(logger, rai_config, q.load_resources(logger, env_config, resources, src), readonly=False)
        except KeyError as e:
            logger.error(f"Unsupported file type: {src['file_type']}. Skip the source: {src}", e)
        except ValueError as e:
            logger.error(f"Unsupported source type. Skip the source: {src}", e)


class LoadDataWorkflowStepFactory(WorkflowStepFactory):

    def _required_params(self, config: WorkflowConfig) -> List[str]:
        return [constants.COLLAPSE_PARTITIONS_ON_LOAD]

    def _get_step(self, logger: logging.Logger, config: WorkflowConfig, idt, name, state, timing, engine_size,
                  step: dict) -> WorkflowStep:
        collapse_partitions_on_load = config.step_params[constants.COLLAPSE_PARTITIONS_ON_LOAD]
        return LoadDataWorkflowStep(idt, name, state, timing, engine_size, collapse_partitions_on_load)


class MaterializeWorkflowStep(WorkflowStep):
    relations: List[str]
    materialize_jointly: bool

    def __init__(self, idt, name, state, timing, engine_size, relations, materialize_jointly):
        super().__init__(idt, name, state, timing, engine_size)
        self.relations = relations
        self.materialize_jointly = materialize_jointly

    def execute(self, logger: logging.Logger, env_config: EnvConfig, rai_config: RaiConfig):
        logger.info("Executing Materialize step..")

        if self.materialize_jointly:
            rai.execute_query(logger, rai_config, q.materialize(self.relations), readonly=False)
        else:
            for relation in self.relations:
                rai.execute_query(logger, rai_config, q.materialize([relation]), readonly=False)


class MaterializeWorkflowStepFactory(WorkflowStepFactory):

    def _get_step(self, logger: logging.Logger, config: WorkflowConfig, idt, name, state, timing, engine_size,
                  step: dict) -> WorkflowStep:
        return MaterializeWorkflowStep(idt, name, state, timing, engine_size, step["relations"],
                                       step["materializeJointly"])


class ExportWorkflowStep(WorkflowStep):
    exports: List[Export]
    export_jointly: bool

    def __init__(self, idt, name, state, timing, engine_size, exports, export_jointly):
        super().__init__(idt, name, state, timing, engine_size)
        self.exports = exports
        self.export_jointly = export_jointly

    def execute(self, logger: logging.Logger, env_config: EnvConfig, rai_config: RaiConfig):
        logger.info("Executing Export step..")

        if self.export_jointly:
            self._export(logger, env_config, rai_config, self.exports)
        else:
            for export in self.exports:
                self._export(logger, env_config, rai_config, [export])

    def _export(self, logger: logging.Logger, env_config: EnvConfig, rai_config: RaiConfig, exports):
        raise NotImplementedError("This class is abstract")


class ExportWorkflowStepFactory(WorkflowStepFactory):

    def _required_params(self, config: WorkflowConfig) -> List[str]:
        required_params = []
        if config.run_mode == WorkflowRunMode.LOCAL:
            required_params.append(constants.OUTPUT_ROOT)
        elif config.run_mode == WorkflowRunMode.REMOTE:
            required_params.append(constants.END_DATE)
        return required_params

    def _get_step(self, logger: logging.Logger, config: WorkflowConfig, idt, name, state, timing, engine_size,
                  step: dict) -> WorkflowStep:
        exports = self._load_exports(logger, step)
        if config.run_mode == WorkflowRunMode.LOCAL:
            output_root = config.step_params[constants.OUTPUT_ROOT]
            return LocalExportWorkflowStep(idt, name, state, timing, engine_size, exports, step["exportJointly"],
                                           output_root)
        elif config.run_mode == WorkflowRunMode.REMOTE:
            end_date = config.step_params[constants.END_DATE]
            return RemoteExportWorkflowStep(idt, name, state, timing, engine_size, exports, step["exportJointly"],
                                            step["dateFormat"], end_date)
        else:
            raise Exception("Unsupported mode")

    @staticmethod
    def _load_exports(logger: logging.Logger, src) -> List[Export]:
        exports_json = src["exports"]
        exports = []
        for e in exports_json:
            if "future" not in e or not e["future"]:
                try:
                    meta_key = e["metaKey"] if "metaKey" in e else []
                    file_type_str = e["type"].upper()
                    offset_by_number_of_days = e["offsetByNumberOfDays"] if "offsetByNumberOfDays" in e else 0
                    cfg = Export(meta_key, e["configRelName"], e["relativePath"], FileType[file_type_str],
                                 offset_by_number_of_days)
                    exports.append(cfg)
                except KeyError as ex:
                    logger.warning(f"Unsupported FileType: {ex}. Skipping export: {e}.")
        return exports


class LocalExportWorkflowStep(ExportWorkflowStep):
    output_root: str

    def __init__(self, idt, name, state, timing, engine_size, exports, export_jointly, output_root):
        super().__init__(idt, name, state, timing, engine_size, exports, export_jointly)
        self.output_root = output_root

    def _export(self, logger: logging.Logger, env_config: EnvConfig, rai_config: RaiConfig, exports):
        save_csv_output(
            rai.execute_query_csv(logger, rai_config, q.export_relations_local(logger, exports)),
            self.output_root)


class RemoteExportWorkflowStep(ExportWorkflowStep):
    date_format: str
    end_date: str

    def __init__(self, idt, name, state, timing, engine_size, exports, export_jointly, date_format, end_date):
        super().__init__(idt, name, state, timing, engine_size, exports, export_jointly)
        self.end_date = end_date
        self.date_format = date_format

    def _export(self, logger: logging.Logger, env_config: EnvConfig, rai_config: RaiConfig, exports):
        rai.execute_query(
            logger, rai_config,
            q.export_relations_remote(logger, env_config, exports, self.end_date, self.date_format))


DEFAULT_FACTORIES = MappingProxyType(
    {
        CONFIGURE_SOURCES: ConfigureSourcesWorkflowStepFactory(),
        INSTALL_MODELS: InstallModelWorkflowStepFactory(),
        LOAD_DATA: LoadDataWorkflowStepFactory(),
        MATERIALIZE: MaterializeWorkflowStepFactory(),
        EXPORT: ExportWorkflowStepFactory()
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
                    self.logger.info(f"Step {step.name} (id='{step.idt}') is not selected. Skipping..")
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
            rai.execute_query(self.logger, rai_config,
                              q.update_step_state(step.idt, WorkflowStepState.IN_PROGRESS.name), readonly=False,
                              ignore_problems=True)
            try:
                if step.engine_size:
                    self.resource_manager.add_engine(step.engine_size)
                    step.execute(self.logger, self.config.env, self.resource_manager.get_rai_config(step.engine_size))
                    next_step = steps_iter.peek(None)
                    if next_step and next_step.engine_size != step.engine_size:
                        self.resource_manager.remove_engine(step.engine_size)
                else:
                    step.execute(self.logger, self.config.env, rai_config)

                end_time = time.time()
                execution_time = end_time - start_time
                query = "\n".join([q.update_step_state(step.idt, WorkflowStepState.SUCCESS.name),
                                   q.update_execution_time(step.idt, execution_time)])
                rai.execute_query(self.logger, rai_config, query, readonly=False)
            except Exception as e:
                rai.execute_query(self.logger, rai_config,
                                  q.update_step_state(step.idt, WorkflowStepState.FAILED.name), readonly=False,
                                  ignore_problems=True)
                if step.engine_size:
                    self.resource_manager.remove_engine(step.engine_size)
                raise e

            self.logger.info(f"{step.name} (id='{step.idt}') finished in {format_duration(execution_time)}")

    def print_timings(self) -> None:
        rai_config = self.resource_manager.get_rai_config()
        relation = build_relation_path(constants.WORKFLOW_JSON_REL, self.config.batch_config.name)
        workflow_info = rai.execute_relation_json(self.logger, rai_config, relation, ignore_problems=True)
        steps = workflow_info["steps"]
        for step in steps:
            execution_time = step["executionTime"]
            self.logger.info(f"{step['name']} (id={step['idt']}) finished in {format_duration(execution_time)}")
        self.logger.info(f"Total workflow execution time is {format_duration(workflow_info['totalTime'])}")

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
            rai.install_models(logger, rai_config, extended_models)
            # Load batch config
            rai.load_json(logger, rai_config,
                          build_relation_path(constants.CONFIG_BASE_RELATION, config.batch_config.name),
                          config.batch_config.content)
            # Init workflow steps
            rai.execute_query(logger, rai_config, q.init_workflow_steps(config.batch_config.name), readonly=False)

        extended_factories = {**DEFAULT_FACTORIES, **factories}
        workflow_info = rai.execute_relation_json(logger, rai_config,
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
