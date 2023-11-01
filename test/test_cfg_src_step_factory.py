import logging
import unittest
from unittest.mock import Mock, MagicMock

from workflow import paths
from workflow import constants
from workflow.common import Source, BatchConfig, EnvConfig, Container, ContainerType
from workflow.executor import ConfigureSourcesWorkflowStepFactory, WorkflowConfig, WorkflowStepState


class TestConfigureSourcesWorkflowStepFactory(unittest.TestCase):
    logger: logging.Logger = Mock()

    def test_get_step(self):
        # Setup factory spy
        factory = ConfigureSourcesWorkflowStepFactory()
        config = _create_wf_cfg(EnvConfig({"azure": _create_container("default", ContainerType.AZURE),
                                           "local": _create_container("local", ContainerType.LOCAL)}), Mock())
        spy = MagicMock(wraps=factory._parse_sources)
        sources = [_create_test_source(config.env.get_container("azure"), "src1"),
                   _create_test_source(config.env.get_container("local"), "src2"),
                   _create_test_source(config.env.get_container("local"), "src3"),
                   _create_test_source(config.env.get_container("local"), "src4")]
        spy.return_value = sources
        factory._parse_sources = spy

        # When
        step = factory._get_step(self.logger, config, "1", "name", "ConfigureSources", WorkflowStepState.INIT, 0, None,
                                 {"configFiles": []})
        # Then
        self.assertEqual("1", step.idt)
        self.assertEqual("name", step.name)
        self.assertEqual(WorkflowStepState.INIT, step.state)
        self.assertEqual(0, step.timing)
        self.assertEqual(None, step.engine_size)
        self.assertEqual([], step.config_files)
        self.assertEqual("./rel", step.rel_config_dir)
        self.assertEqual(sources, step.sources)
        self.assertEqual(2, len(step.paths_builders.keys()))
        self.assertIsInstance(step.paths_builders.get("local"), paths.LocalPathsBuilder)
        self.assertIsInstance(step.paths_builders.get("default"), paths.AzurePathsBuilder)
        self.assertEqual("2021-01-01", step.start_date)
        self.assertEqual("2021-01-01", step.end_date)
        self.assertFalse(step.force_reimport)
        self.assertFalse(step.force_reimport_not_chunk_partitioned)


def _create_test_source(container: Container, relation: str) -> Source:
    return Source(
        container=container,
        relation=relation,
        relative_path="test",
        input_format="test",
        extensions=["test"],
        is_chunk_partitioned=True,
        is_date_partitioned=True,
        loads_number_of_days=0,
        offset_by_number_of_days=0,
        snapshot_validity_days=0,
        paths=[]
    )


def _create_container(name: str, c_type: ContainerType) -> Container:
    return Container(
        name=name,
        type=c_type,
        params={}
    )


def _create_wf_cfg(env_config: EnvConfig, batch_config: BatchConfig) -> WorkflowConfig:
    parameters = {
        constants.REL_CONFIG_DIR: "./rel",
        constants.START_DATE: "2021-01-01",
        constants.END_DATE: "2021-01-01",
        constants.FORCE_REIMPORT: False,
        constants.FORCE_REIMPORT_NOT_CHUNK_PARTITIONED: False,
        constants.COLLAPSE_PARTITIONS_ON_LOAD: False
    }
    return WorkflowConfig(
        env=env_config,
        batch_config=batch_config,
        recover=False,
        recover_step="",
        selected_steps=[],
        step_params=parameters
    )
