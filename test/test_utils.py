import unittest
import logging

import workflow.utils


class UtilsTest(unittest.TestCase):
    logger: logging.Logger

    def test_should_extract_range_when_end_date(self):
        # when
        result = workflow.utils.extract_date_range(self.logger, None, "20220105", None, None)
        # then
        self.assertEqual(["20220105"], result)

    def test_should_extract_range_when_end_and_start_date(self):
        # when
        result = workflow.utils.extract_date_range(self.logger, "20220103", "20220105", None, None)
        # then
        self.assertEqual(["20220103", "20220104", "20220105"], result)

    def test_should_extract_range_based_on_end_date_with_given_offset(self):
        # when
        result = workflow.utils.extract_date_range(self.logger, None, "20220105", None, 1)
        # then
        self.assertEqual(["20220104"], result)

    def test_should_extract_range_when_end_date_with_number_of_days(self):
        # when
        result = workflow.utils.extract_date_range(self.logger, None, "20220105", 2, None)
        # then
        self.assertEqual(["20220104", "20220105"], result)

    def test_should_extract_range_when_end_and_start_date_with_given_offset(self):
        # when
        result = workflow.utils.extract_date_range(self.logger, "20220104", "20220105", None, 1)
        # then
        self.assertEqual(["20220104"], result)

    def test_should_extract_range_when_end_and_start_date_with_number_of_days_less_then_start_date_period(self):
        # when
        result = workflow.utils.extract_date_range(self.logger, "20220101", "20220105", 2, None)
        # then
        self.assertEqual(["20220104", "20220105"], result)

    def test_should_extract_range_when_end_and_start_date_with_number_of_days_greater_then_start_date_period(self):
        # when
        result = workflow.utils.extract_date_range(self.logger, "20220104", "20220105", 5, None)
        # then
        self.assertEqual(["20220104", "20220105"], result)

    def test_should_extract_range_when_end_and_start_date_with_given_offset_and_number_of_days(self):
        # when
        result = workflow.utils.extract_date_range(self.logger, "20220101", "20220105", 2, 1)
        # then
        self.assertEqual(["20220103", "20220104"], result)

    def test_should_extract_empty_range_when_end_date_earlier_then_start_date(self):
        # when
        result = workflow.utils.extract_date_range(self.logger, "20220106", "20220105", None, None)
        # then
        self.assertEqual([], result)

    @classmethod
    def setUpClass(cls) -> None:
        cls.logger = logging.getLogger("utils-test")
        cls.logger.setLevel(logging.INFO)
        cls.logger.addHandler(logging.StreamHandler())
