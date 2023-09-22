import logging
import unittest
import uuid
from datetime import datetime
from typing import List
from unittest.mock import Mock, patch

from workflow.common import Export, RaiConfig, FileType
from workflow.executor import WorkflowStepState, ExportWorkflowStep


class TestConfigureSourcesWorkflowStep(unittest.TestCase):
    logger: logging.Logger = Mock()

    @patch('workflow.rai.execute_query_take_single')
    def test_should_export_should_not_export_valid_snapshot(self,  mock_execute_query):
        # given
        export = Export([], "relation", "relative_path", FileType.CSV, "snapshot_binding", "default")
        end_date = "20220105"
        step = _create_export_step([export], end_date)
        rai_config = RaiConfig(Mock(), "engine", "database")

        mock_execute_query.return_value = "20220106"  # valid until end_date + 1
        # when
        should_export = step._should_export(self.logger, rai_config, export)
        # then
        self.assertFalse(should_export)

    @patch('workflow.rai.execute_query_take_single')
    def test_should_export_should_export_snapshot_expiring_today(self,  mock_execute_query):
        # given
        export = Export([], "relation", "relative_path", FileType.CSV, "snapshot_binding", "default")
        end_date = "20220105"
        step = _create_export_step([export], end_date)
        rai_config = RaiConfig(Mock(), "engine", "database")

        mock_execute_query.return_value = "20220105"  # valid until end_date
        # when
        should_export = step._should_export(self.logger, rai_config, export)
        # then
        self.assertTrue(should_export)

    @patch('workflow.rai.execute_query_take_single')
    def test_should_export_should_export_expired_snapshot(self,  mock_execute_query):
        # given
        export = Export([], "relation", "relative_path", FileType.CSV, "snapshot_binding", "default")
        end_date = "20220105"
        step = _create_export_step([export], end_date)
        rai_config = RaiConfig(Mock(), "engine", "database")

        mock_execute_query.return_value = "20220101"  # valid until end_date
        # when
        should_export = step._should_export(self.logger, rai_config, export)
        # then
        self.assertTrue(should_export)


def _create_export_step(exports: List[Export], end_date: str, export_jointly: bool = True,
                        date_format: str = "%Y%m%d") -> ExportWorkflowStep:
    return ExportWorkflowStep(
        idt=uuid.uuid4(),
        name="test",
        state=WorkflowStepState.INIT,
        timing=datetime.now(),
        engine_size="xs",
        exports=exports,
        export_jointly=export_jointly,
        date_format=date_format,
        end_date=end_date
    )
