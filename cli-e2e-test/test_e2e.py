import unittest
import os
import logging
import shutil
import uuid

import test_query as q
import workflow.manager
import workflow.rai
from csv_diff import load_csv, compare, human_text
from subprocess import call


class CliE2ETest(unittest.TestCase):
    logger: logging.Logger
    resource_manager: workflow.manager.ResourceManager
    output = "./output"
    dev_data_dir = "./data"
    temp_folder = f"{dev_data_dir}/temp"
    env_config = "./config/loader.toml"
    expected = "./expected_results"
    resource_name = "wm-cli-e2e-test-" + str(uuid.uuid4())
    cmd_with_common_arguments = ["python", "main.py",
                                 "--run-mode", "local",
                                 "--env-config", env_config,
                                 "--engine", resource_name,
                                 "--database", resource_name,
                                 "--rel-config-dir", "./rel",
                                 "--dev-data-dir", dev_data_dir,
                                 "--output-root", output]

    def test_scenario1_model(self):
        # when
        test_args = ["--batch-config", "./config/model/scenario1.json",
                     "--end-date", "20220105",
                     "--drop-db"]
        rsp = call(self.cmd_with_common_arguments + test_args)
        # then
        self.assertNotEqual(rsp, 1)
        self.assert_output_dir_files(self.test_scenario1_model.__name__)

    def test_scenario2_model_no_data_changes(self):
        # when
        test_args = ["--batch-config", "./config/model/scenario2.json",
                     "--start-date", "20230908",
                     "--end-date", "20230909"]
        rsp = call(self.cmd_with_common_arguments + test_args + ["--drop-db"])
        # then
        self.assertNotEqual(rsp, 1)
        # and when
        rsp = call(self.cmd_with_common_arguments + test_args)
        # then
        self.assertNotEqual(rsp, 1)
        rai_config = self.resource_manager.get_rai_config()
        rsp_json = workflow.rai.execute_relation_json(self.logger, rai_config, q.RESOURCES_TO_DELETE)
        self.assertEqual(rsp_json, {})

    def test_scenario2_model_force_reimport(self):
        # when
        test_args = ["--batch-config", "./config/model/scenario2.json",
                     "--start-date", "20230908",
                     "--end-date", "20230909"]
        rsp = call(self.cmd_with_common_arguments + test_args + ["--drop-db"])
        # then
        self.assertNotEqual(rsp, 1)
        # and when
        rsp = call(self.cmd_with_common_arguments + test_args + ["--force-reimport"])
        # then
        self.assertNotEqual(rsp, 1)
        rai_config = self.resource_manager.get_rai_config()
        rsp_json = workflow.rai.execute_relation_json(self.logger, rai_config, q.RESOURCES_TO_DELETE)
        self.assertEqual(rsp_json, [{'partition': 2023090800001, 'relation': 'city_data'},
                                    {'partition': 2023090800002, 'relation': 'city_data'},
                                    {'partition': 2023090900001, 'relation': 'city_data'},
                                    {'relation': 'zip_city_state_master_data'}])

    def test_scenario2_model_force_reimport_chunk_partitioned(self):
        # when
        test_args = ["--batch-config", "./config/model/scenario2.json",
                     "--start-date", "20230908",
                     "--end-date", "20230909"]
        rsp = call(self.cmd_with_common_arguments + test_args + ["--drop-db"])
        # then
        self.assertNotEqual(rsp, 1)
        # and when
        rsp = call(self.cmd_with_common_arguments + test_args + ["--force-reimport-not-chunk-partitioned"])
        # then
        self.assertNotEqual(rsp, 1)
        rai_config = self.resource_manager.get_rai_config()
        rsp_json = workflow.rai.execute_relation_json(self.logger, rai_config, q.RESOURCES_TO_DELETE)
        self.assertEqual(rsp_json, [{'relation': 'zip_city_state_master_data'}])

    def test_scenario3_model_single_partition_change(self):
        # when
        test_args = ["--batch-config", "./config/model/scenario3.json",
                     "--start-date", "20230908",
                     "--end-date", "20230909"]
        # copy data for scenario 3
        data_folder = "/city"
        shutil.copytree(f"{self.dev_data_dir}{data_folder}", f"{self.temp_folder}{data_folder}")
        rsp = call(self.cmd_with_common_arguments + test_args + ["--drop-db"])
        # then
        self.assertNotEqual(rsp, 1)
        # and when
        # rename files to simulate data refresh
        os.rename(f"{self.temp_folder}{data_folder}/data_dt=20230908/part-1-3d1ec0b0-ebfd-a773-71d7-f71f42a2f066.csv",
                  f"{self.temp_folder}{data_folder}/data_dt=20230908/part-1-{uuid.uuid4()}.csv")
        rsp = call(self.cmd_with_common_arguments + test_args)
        # then
        self.assertNotEqual(rsp, 1)
        rai_config = self.resource_manager.get_rai_config()
        rsp_json = workflow.rai.execute_relation_json(self.logger, rai_config, q.RESOURCES_TO_DELETE)
        self.assertEqual(rsp_json, [{'partition': 2023090800001, 'relation': 'city_data'}])

    def test_scenario3_model_two_partitions_overriden_by_one(self):
        # when
        test_args = ["--batch-config", "./config/model/scenario3.json",
                     "--start-date", "20230908",
                     "--end-date", "20230909"]
        data_folder = "/city"
        # copy data for scenario 3
        shutil.copytree(f"{self.dev_data_dir}{data_folder}", f"{self.temp_folder}{data_folder}")
        rsp = call(self.cmd_with_common_arguments + test_args + ["--drop-db"])
        # then
        self.assertNotEqual(rsp, 1)
        # and when
        # rename files to simulate data refresh
        os.rename(f"{self.temp_folder}{data_folder}/data_dt=20230908/part-1-3d1ec0b0-ebfd-a773-71d7-f71f42a2f066.csv",
                  f"{self.temp_folder}{data_folder}/data_dt=20230908/part-1-{uuid.uuid4()}.csv")
        os.remove(f"{self.temp_folder}{data_folder}/data_dt=20230908/part-2-3d9ec0b0-ebfd-a773-71d7-f71f42a2f066.csv")
        rsp = call(self.cmd_with_common_arguments + test_args)
        # then
        self.assertNotEqual(rsp, 1)
        rai_config = self.resource_manager.get_rai_config()
        rsp_json = workflow.rai.execute_relation_json(self.logger, rai_config, q.RESOURCES_TO_DELETE)
        self.assertEqual(rsp_json, [{'partition': 2023090800001, 'relation': 'city_data'},
                                    {'partition': 2023090800002, 'relation': 'city_data'}])

    def test_scenario4_model_reimport_2_partitions_data_with_1(self):
        # when
        test_args = ["--batch-config", "./config/model/scenario4.json",
                     "--start-date", "20230908",
                     "--end-date", "20230908"]
        data_folder = "/city"
        # copy data for scenario 3
        shutil.copytree(f"{self.dev_data_dir}{data_folder}", f"{self.temp_folder}{data_folder}")
        rsp = call(self.cmd_with_common_arguments + test_args + ["--drop-db"])
        # then
        self.assertNotEqual(rsp, 1)
        # and when
        # replace files to simulate data refresh
        shutil.rmtree(f"{self.temp_folder}{data_folder}/data_dt=20230908")
        shutil.copytree(f"{self.dev_data_dir}{data_folder}/data_dt=20230909",
                        f"{self.temp_folder}{data_folder}/data_dt=20230908")
        rsp = call(self.cmd_with_common_arguments + test_args)
        # then
        self.assertNotEqual(rsp, 1)
        self.assert_output_dir_files(self.test_scenario4_model_reimport_2_partitions_data_with_1.__name__)

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
        if os.path.exists(self.temp_folder):
            shutil.rmtree(self.temp_folder)

    @classmethod
    def tearDownClass(cls) -> None:
        cls.resource_manager.cleanup_resources()

    def assert_output_dir_files(self, scenario: str):
        for filename in os.listdir(f"{self.output}"):
            actual_path = os.path.join(self.output, filename)
            expected_path = os.path.join(f"{self.expected}/{scenario}", filename)
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
