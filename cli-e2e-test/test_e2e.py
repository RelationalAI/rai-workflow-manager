import unittest
import os
import logging
import uuid

import workflow.manager
from csv_diff import load_csv, compare, human_text
from subprocess import call


class CliE2ETest(unittest.TestCase):
    logger: logging.Logger
    resource_manager: workflow.manager.ResourceManager
    output = "./output"
    env_config = "./config/loader.toml"
    expected = "./expected_results"
    resource_name = "wm-cli-e2e-test-" + str(uuid.uuid4())
    cmd_with_common_arguments = ["python", "main.py",
                                 "--run-mode", "local",
                                 "--env-config", env_config,
                                 "--engine", resource_name,
                                 "--database", resource_name,
                                 "--rel-config-dir", "./rel",
                                 "--dev-data-dir", "./data",
                                 "--output-root", output]

    def test_scamp_aggregates_model(self):
        # when
        test_args = ["--batch-config", "./config/model/scenario1.json",
                     "--start-date", "20220103",
                     "--end-date", "20220105"]
        command = self.cmd_with_common_arguments
        command += test_args
        rsp = call(command)
        # then
        self.assertNotEqual(rsp, 1)
        self.assert_output_dir_files()

    @classmethod
    def setUpClass(cls) -> None:
        # Make sure output folder is empty since the folder share across repository. Remove README.md, other files left.
        cleanup_output(cls.output)
        cls.logger = logging.getLogger("cli-e2e-test")
        cls.resource_manager = workflow.manager.ResourceManager.init(cls.logger, cls.resource_name, cls.resource_name)
        cls.logger.setLevel(logging.INFO)
        cls.logger.addHandler(logging.StreamHandler())
        cls.resource_manager.add_engine()

    def tearDown(self):
        cleanup_output(self.output)

    @classmethod
    def tearDownClass(cls) -> None:
        cls.resource_manager.cleanup_resources()

    def assert_output_dir_files(self):
        for filename in os.listdir(self.output):
            actual_path = os.path.join(self.output, filename)
            expected_path = os.path.join( self.expected, filename)
            with open(actual_path, 'r') as actual, open(expected_path, 'r') as expected:
                diff = compare(
                    load_csv(actual),
                    load_csv(expected)
                )
                self.logger.info(f"Assert file `{filename}`")
                self.assertEqual(human_text(diff), '')


def cleanup_output(directory: str):
    for filename in os.listdir(directory):
        file_path = os.path.join(directory, filename)
        if os.path.isfile(file_path):
            os.remove(file_path)
