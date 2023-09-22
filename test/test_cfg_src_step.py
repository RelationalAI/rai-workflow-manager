import logging
import unittest
import uuid
from datetime import datetime
from typing import List
from unittest.mock import Mock

from workflow import paths
from workflow.common import Source
from workflow.executor import ConfigureSourcesWorkflowStep, WorkflowStepState


class TestConfigureSourcesWorkflowStep(unittest.TestCase):
    logger: logging.Logger = Mock()

    #
    # Not partitioned file tests
    #

    def test_get_date_range_not_part(self):
        # Look up non-partitioned files
        test_src = _create_test_source(
            is_date_partitioned=False,
            is_chunk_partitioned=False,
        )
        paths_builder = Mock()
        workflow_step = _create_cfg_sources_step([test_src], {"default": paths_builder}, None, None)

        # When calling _get_date_range
        days = workflow_step._get_date_range(self.logger, test_src)
        # Then
        expected_days = []
        self.assertEqual(expected_days, days)

    def test_inflate_sources_not_part(self):
        # Look up non-partitioned file parts
        test_src = _create_test_source(
            is_date_partitioned=False,
            is_chunk_partitioned=False,
        )
        paths_builder = _create_path_builder_mock([
            paths.FilePath(path="test/test_non_part.csv"),
        ])
        workflow_step = _create_cfg_sources_step([test_src], {"default": paths_builder}, None, None)
        # When calling _inflate_sources
        workflow_step._inflate_sources(self.logger)
        # Then
        expected_paths = [
            "test/test_non_part.csv",
        ]
        self.assertEqual(expected_paths, test_src.paths)

    #
    # Date-partitioned file tests
    #

    def test_get_date_range_date_part_1day(self):
        # Look up date-partitioned files from the last day
        test_src = _create_test_source(
            loads_number_of_days=1,
            offset_by_number_of_days=0,
        )
        end_date = "20220105"
        paths_builder = Mock()
        workflow_step = _create_cfg_sources_step([test_src], {"default": paths_builder}, None, end_date)

        # When calling _get_date_range
        days = workflow_step._get_date_range(self.logger, test_src)
        # Then
        expected_days = ["20220105"]
        self.assertEqual(expected_days, days)

    def test_inflate_sources_date_part_1day_multiple_paths(self):
        # Look up date-partitioned file paths from the last day
        test_src = _create_test_source(
            loads_number_of_days=1,
            offset_by_number_of_days=0,
        )
        paths_builder = _create_path_builder_mock([
            paths.FilePath(path="test/test_20220103_1.csv", as_of_date="20220103"),
            paths.FilePath(path="test/test_20220104_1.csv", as_of_date="20220104"),
            paths.FilePath(path="test/test_20220104_2.csv", as_of_date="20220104"),
            paths.FilePath(path="test/test_20220105_1.csv", as_of_date="20220105"),
            paths.FilePath(path="test/test_20220105_2.csv", as_of_date="20220105"),
            paths.FilePath(path="test/test_20220105_3.csv", as_of_date="20220105"),
        ])
        end_date = "20220105"
        workflow_step = _create_cfg_sources_step([test_src], {"default": paths_builder}, None, end_date)
        # When calling _inflate_sources
        workflow_step._inflate_sources(self.logger)
        # Then
        expected_paths = [
            "test/test_20220105_1.csv",
            "test/test_20220105_2.csv",
            "test/test_20220105_3.csv",
        ]
        self.assertEqual(expected_paths, test_src.paths)

    def test_get_date_range_date_part_10days(self):
        # Look up date-partitioned files from the last 10 days
        test_src = _create_test_source(
            loads_number_of_days=10,
            offset_by_number_of_days=0,
        )
        end_date = "20220115"
        paths_builder = Mock()
        workflow_step = _create_cfg_sources_step([test_src], {"default": paths_builder}, None, end_date)

        # When calling _get_date_range
        days = workflow_step._get_date_range(self.logger, test_src)
        # Then
        expected_days = [f"{day}" for day in range(20220106, 20220116)]  # 20220106, 20220107, ..., 20220115
        self.assertEqual(expected_days, days)

    def test_inflate_sources_date_part_10days(self):
        # Look up date-partitioned file paths from the last 10 days
        test_src = _create_test_source(
            loads_number_of_days=10,
            offset_by_number_of_days=0,
        )
        paths_builder = _create_path_builder_mock([
            paths.FilePath(path=f"test/test_{day}_1.csv", as_of_date=f"{day}") for day in range(20220101, 20220116)
        ])  # 20220101, 20220102, ..., 20220115
        end_date = "20220115"
        workflow_step = _create_cfg_sources_step([test_src], {"default": paths_builder}, None, end_date)
        # When calling _inflate_sources
        workflow_step._inflate_sources(self.logger)
        # Then
        expected_paths = [f"test/test_{day}_1.csv" for day in range(20220106, 20220116)]
        self.assertEqual(expected_paths, test_src.paths)

    def test_get_date_range_date_part_10days_offset_2days(self):
        # Look up date-partitioned files from the last 10 days offset by 2 days
        test_src = _create_test_source(
            loads_number_of_days=10,
            offset_by_number_of_days=2,
        )
        end_date = "20220112"
        paths_builder = Mock()
        workflow_step = _create_cfg_sources_step([test_src], {"default": paths_builder}, None, end_date)

        # When calling _get_date_range
        days = workflow_step._get_date_range(self.logger, test_src)
        # Then
        expected_days = [f"{day}" for day in range(20220101, 20220111)]  # 20220101, 20220102, ..., 20220110
        self.assertEqual(expected_days, days)

    def test_inflate_sources_date_part_10days_offset_2days(self):
        # Look up date-partitioned file paths from the last 10 days offset by 2 days
        test_src = _create_test_source(
            loads_number_of_days=10,
            offset_by_number_of_days=2,
        )
        paths_builder = _create_path_builder_mock([
            paths.FilePath(path=f"test/test_{day}_{i}.csv", as_of_date=f"{day}") for day in range(20220101, 20220111)
            for i in range(2)
        ])  # 20220101, 20220102, ..., 20220110
        end_date = "20220112"
        workflow_step = _create_cfg_sources_step([test_src], {"default": paths_builder}, None, end_date)
        # When calling _inflate_sources
        workflow_step._inflate_sources(self.logger)
        # Then
        # 20220101, 20220102, ..., 20220110
        expected_paths = [f"test/test_{day}_{i}.csv" for day in range(20220101, 20220111) for i in range(2)]
        self.assertEqual(expected_paths, test_src.paths)

    #
    # Snapshot file tests
    #

    def test_get_date_range_snapshot_valid_the_same_day(self):
        # Look up snapshot files for the same day only
        test_src = _create_test_source(
            loads_number_of_days=1,
            offset_by_number_of_days=0,
            snapshot_validity_days=0
        )
        end_date = "20220105"
        paths_builder = Mock()
        workflow_step = _create_cfg_sources_step([test_src], {"default": paths_builder}, None, end_date)

        # When calling _get_date_range
        days = workflow_step._get_date_range(self.logger, test_src)
        # Then
        expected_days = ["20220105"]
        self.assertEqual(expected_days, days)

    def test_inflate_sources_snapshot_valid_the_same_day(self):
        # Look up snapshot file paths from the same day
        test_src = _create_test_source(
            loads_number_of_days=1,
            offset_by_number_of_days=0,
            snapshot_validity_days=0
        )
        paths_builder = _create_path_builder_mock([
            paths.FilePath(path="test/snapshot_20220103.csv", as_of_date="20220103"),
            paths.FilePath(path="test/snapshot_20220104.csv", as_of_date="20220104"),
            paths.FilePath(path="test/snapshot_20220105.csv", as_of_date="20220105"),
        ])
        end_date = "20220105"
        workflow_step = _create_cfg_sources_step([test_src], {"default": paths_builder}, None, end_date)
        # When calling _inflate_sources
        workflow_step._inflate_sources(self.logger)
        # Then
        expected_paths = [
            "test/snapshot_20220105.csv",
        ]
        self.assertEqual(expected_paths, test_src.paths)

    def test_get_date_range_snapshot_1day(self):
        # Look up snapshot files for the last 1 day
        test_src = _create_test_source(
            loads_number_of_days=1,
            offset_by_number_of_days=0,
            snapshot_validity_days=1
        )
        end_date = "20220105"
        paths_builder = Mock()
        workflow_step = _create_cfg_sources_step([test_src], paths_builder, None, end_date)

        # When calling _get_date_range
        days = workflow_step._get_date_range(self.logger, test_src)
        # Then
        expected_days = ["20220104", "20220105"]  # valid two days
        self.assertEqual(expected_days, days)

    def test_inflate_sources_snapshot_1day(self):
        # Look up snapshot file paths from the last day
        test_src = _create_test_source(
            loads_number_of_days=1,
            offset_by_number_of_days=0,
            snapshot_validity_days=1
        )
        paths_builder = _create_path_builder_mock([
            paths.FilePath(path="test/snapshot_20220102.csv", as_of_date="20220102"),
            paths.FilePath(path="test/snapshot_20220103.csv", as_of_date="20220103"),
            paths.FilePath(path="test/snapshot_20220104.csv", as_of_date="20220104"),
            paths.FilePath(path="test/snapshot_20220105.csv", as_of_date="20220105"),
        ])
        end_date = "20220105"
        workflow_step = _create_cfg_sources_step([test_src], {"default": paths_builder}, None, end_date)
        # When calling _inflate_sources
        workflow_step._inflate_sources(self.logger)
        # Then
        expected_paths = [
            "test/snapshot_20220105.csv",
        ]
        self.assertEqual(expected_paths, test_src.paths)

    def test_get_date_range_snapshot_1day_offset_by_1day(self):
        # Look up snapshot files for the last 1 day
        test_src = _create_test_source(
            loads_number_of_days=1,
            offset_by_number_of_days=1,
            snapshot_validity_days=1
        )
        end_date = "20220105"
        paths_builder = Mock()
        workflow_step = _create_cfg_sources_step([test_src], {"default": paths_builder}, None, end_date)

        # When calling _get_date_range
        days = workflow_step._get_date_range(self.logger, test_src)
        # Then
        expected_days = ["20220104"]
        self.assertEqual(expected_days, days)

    def test_inflate_sources_snapshot_1day_offset_by_1day(self):
        # Look up snapshot file paths from the last day offset by 1 day
        test_src = _create_test_source(
            loads_number_of_days=1,
            offset_by_number_of_days=1,
            snapshot_validity_days=1
        )
        paths_builder = _create_path_builder_mock([
            paths.FilePath(path="test/snapshot_20220102.csv", as_of_date="20220102"),
            paths.FilePath(path="test/snapshot_20220103.csv", as_of_date="20220103"),
            paths.FilePath(path="test/snapshot_20220104.csv", as_of_date="20220104"),
        ])
        end_date = "20220105"
        workflow_step = _create_cfg_sources_step([test_src], {"default": paths_builder}, None, end_date)
        # When calling _inflate_sources
        workflow_step._inflate_sources(self.logger)
        # Then
        expected_paths = [
            "test/snapshot_20220104.csv",
        ]
        self.assertEqual(expected_paths, test_src.paths)

    def test_get_date_range_snapshot_30days_offset_by_1day(self):
        # Look up snapshot files for the last 30 days
        test_src = _create_test_source(
            loads_number_of_days=1,
            offset_by_number_of_days=1,
            snapshot_validity_days=30
        )
        end_date = "20220131"
        paths_builder = Mock()
        workflow_step = _create_cfg_sources_step([test_src], paths_builder, None, end_date)

        # When calling _get_date_range
        days = workflow_step._get_date_range(self.logger, test_src)
        # Then
        expected_days = [f"{day}" for day in range(20220101, 20220131)]  # 20220101, 20220102, ..., 20220130
        self.assertEqual(expected_days, days)

    def test_get_date_range_snapshot_30days_before_start(self):
        # Look up snapshot file paths last 30 days before start date
        test_src = _create_test_source(
            loads_number_of_days=1,
            offset_by_number_of_days=1,
            snapshot_validity_days=30
        )
        paths_builder = _create_path_builder_mock([])
        end_date = "20220131"
        workflow_step = _create_cfg_sources_step([test_src], {"default": paths_builder}, None, end_date)
        # When calling _inflate_sources
        workflow_step._inflate_sources(self.logger)
        # Then
        expected_paths = []
        self.assertEqual(expected_paths, test_src.paths)

    def test_get_date_range_snapshot_30days_at_start(self):
        # Look up snapshot file paths last 30 days at start date
        test_src = _create_test_source(
            loads_number_of_days=1,
            offset_by_number_of_days=1,
            snapshot_validity_days=30
        )
        paths_builder = _create_path_builder_mock([
            paths.FilePath(path="test/snapshot_20220101.csv", as_of_date="20220101"),
        ])
        end_date = "20220131"
        workflow_step = _create_cfg_sources_step([test_src], {"default": paths_builder}, None, end_date)
        # When calling _inflate_sources
        workflow_step._inflate_sources(self.logger)
        # Then
        expected_paths = ["test/snapshot_20220101.csv"]
        self.assertEqual(expected_paths, test_src.paths)

    def test_get_date_range_snapshot_30days_in_the_middle(self):
        # Look up snapshot file paths last 30 days in the middle
        test_src = _create_test_source(
            loads_number_of_days=1,
            offset_by_number_of_days=1,
            snapshot_validity_days=30
        )
        paths_builder = _create_path_builder_mock([
            paths.FilePath(path="test/snapshot_20220115.csv", as_of_date="20220115"),
        ])
        end_date = "20220131"
        workflow_step = _create_cfg_sources_step([test_src], {"default": paths_builder}, None, end_date)
        # When calling _inflate_sources
        workflow_step._inflate_sources(self.logger)
        # Then
        expected_paths = ["test/snapshot_20220115.csv"]
        self.assertEqual(expected_paths, test_src.paths)

    def test_get_date_range_snapshot_30days_at_the_end(self):
        # Look up snapshot file paths last 30 days at the end
        test_src = _create_test_source(
            loads_number_of_days=1,
            offset_by_number_of_days=1,
            snapshot_validity_days=30
        )
        paths_builder = _create_path_builder_mock([
            paths.FilePath(path="test/snapshot_20220130.csv", as_of_date="20220130"),
        ])
        end_date = "20220131"
        workflow_step = _create_cfg_sources_step([test_src], {"default": paths_builder}, None, end_date)
        # When calling _inflate_sources
        workflow_step._inflate_sources(self.logger)
        # Then
        expected_paths = ["test/snapshot_20220130.csv"]
        self.assertEqual(expected_paths, test_src.paths)

    def test_inflate_sources_snapshot_1day_multiple_paths(self):
        # We look up snapshot files for the last 3 days
        test_src = _create_test_source(
            loads_number_of_days=1,
            offset_by_number_of_days=0,
            snapshot_validity_days=3
        )
        paths_builder = _create_path_builder_mock([
            paths.FilePath(path="test/test_20220103_1.csv", as_of_date="20220103"),
            paths.FilePath(path="test/test_20220104_1.csv", as_of_date="20220104"),
            paths.FilePath(path="test/test_20220105_1.csv", as_of_date="20220105"),
            paths.FilePath(path="test/test_20220105_2.csv", as_of_date="20220105"),
        ])
        end_date = "20220105"
        workflow_step = _create_cfg_sources_step([test_src], {"default": paths_builder}, None, end_date)
        # When calling _inflate_sources
        workflow_step._inflate_sources(self.logger)
        # Then
        expected_paths = [
            "test/test_20220105_1.csv",
            "test/test_20220105_2.csv",
        ]
        self.assertEqual(expected_paths, test_src.paths)


def _create_test_source(is_chunk_partitioned: bool = True, is_date_partitioned: bool = True,
                        loads_number_of_days: int = 1, offset_by_number_of_days: int = 0,
                        snapshot_validity_days=None) -> Source:
    return Source(
        container="default",
        relation="test",
        relative_path="test",
        input_format="test",
        extensions=["test"],
        is_chunk_partitioned=is_chunk_partitioned,
        is_date_partitioned=is_date_partitioned,
        loads_number_of_days=loads_number_of_days,
        offset_by_number_of_days=offset_by_number_of_days,
        snapshot_validity_days=snapshot_validity_days,
        paths=[]
    )


def _create_cfg_sources_step(sources: List[Source], paths_builders: dict[str, paths.PathsBuilder], start_date,
                             end_date) -> ConfigureSourcesWorkflowStep:
    return ConfigureSourcesWorkflowStep(
        idt=uuid.uuid4(),
        name="test",
        state=WorkflowStepState.INIT,
        timing=datetime.now(),
        engine_size="xs",
        config_files=[],
        rel_config_dir="",
        sources=sources,
        paths_builders=paths_builders,
        start_date=start_date,
        end_date=end_date,
        force_reimport=False,
        force_reimport_not_chunk_partitioned=False
    )


def _create_path_builder_mock(file_paths: List[paths.FilePath]) -> paths.PathsBuilder:
    # Create a PathsBuilder mock object with test data
    paths_builder = Mock()
    paths_builder.build = Mock(return_value=file_paths)
    return paths_builder
