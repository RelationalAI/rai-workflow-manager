import unittest
import os
import logging
import shutil
import uuid
import tomli

import workflow.manager
import workflow.rai
import workflow.query
import workflow.common
import workflow.semantic_layer_service
from workflow.constants import RESOURCES_TO_DELETE_REL, DATE_FORMAT
from csv_diff import load_csv, compare, human_text
from subprocess import call


class CliE2ETest(unittest.TestCase):
    logger: logging.Logger
    resource_manager: workflow.manager.ResourceManager
    env_config: workflow.common.EnvConfig
    output = "./output"
    dev_data_dir = "./data"
    temp_folder = f"{dev_data_dir}/temp"
    env_config_path = "./config/loader.toml"
    expected = "./expected_results"
    resource_name = "wm-cli-e2e-test-" + str(uuid.uuid4())
    cmd_with_common_arguments = ["python", "main.py",
                                 "--env-config", env_config_path,
                                 "--engine", resource_name,
                                 "--database", resource_name]

    def test_scenario1_model(self):
        # when
        self.create_workflow("./config/model/scenario1.json")
        self.run_workflow(["--end-date", "20220105"])
        # then
        self.assert_output_dir_files(self.test_scenario1_model.__name__)

    def test_scenario1_model_yaml(self):
        # when
        self.create_workflow("./config/model/scenario1.yaml")
        self.run_workflow(["--end-date", "20220105"])
        # then
        self.assert_output_dir_files(self.test_scenario1_model.__name__)

    def test_scenario2_model_no_data_changes(self):
        # when
        self.create_workflow("./config/model/scenario2.json")
        run_args = ["--start-date", "20230908", "--end-date", "20230909"]
        # first run
        self.run_workflow(run_args)
        # second run
        self.run_workflow(run_args)
        # then
        rai_config = self.resource_manager.get_rai_config()
        rsp_json = workflow.rai.execute_relation_json(self.logger, rai_config, self.env_config, RESOURCES_TO_DELETE_REL)
        self.assertEqual(rsp_json, {})

    def test_scenario2_model_force_reimport(self):
        # when
        self.create_workflow("./config/model/scenario2.json")
        run_args = ["--start-date", "20230908", "--end-date", "20230909"]
        # first run
        self.run_workflow(run_args)
        # second run
        self.run_workflow(run_args + ["--force-reimport"])
        # then
        rai_config = self.resource_manager.get_rai_config()
        rsp_json = workflow.rai.execute_relation_json(self.logger, rai_config, self.env_config, RESOURCES_TO_DELETE_REL)
        self.assertEqual(rsp_json, [{'relation': 'city_data'},
                                    {'relation': 'product_data'},
                                    {'relation': 'zip_city_state_master_data'}])

    def test_scenario2_model_force_reimport_chunk_partitioned(self):
        # when
        self.create_workflow("./config/model/scenario2.json")
        run_args = ["--start-date", "20230908", "--end-date", "20230909"]
        # first run
        self.run_workflow(run_args)
        # second run
        self.run_workflow(run_args + ["--force-reimport-not-chunk-partitioned"])
        # then
        rai_config = self.resource_manager.get_rai_config()
        rsp_json = workflow.rai.execute_relation_json(self.logger, rai_config, self.env_config, RESOURCES_TO_DELETE_REL)
        self.assertEqual(rsp_json, [{'relation': 'zip_city_state_master_data'}])

    def test_scenario3_model_single_partition_change_for_date_partitioned(self):
        # when
        self.create_workflow("./config/model/scenario3.json")
        run_args = ["--start-date", "20230908", "--end-date", "20230909"]
        # first run
        # copy data for scenario 3
        data_folder = "/city"
        shutil.copytree(f"{self.dev_data_dir}{data_folder}", f"{self.temp_folder}{data_folder}")
        self.run_workflow(run_args)
        # second run
        # rename files to simulate data refresh
        os.rename(f"{self.temp_folder}{data_folder}/data_dt=20230908/part-1-3d1ec0b0-ebfd-a773-71d7-f71f42a2f066.csv",
                  f"{self.temp_folder}{data_folder}/data_dt=20230908/part-1-{uuid.uuid4()}.csv")
        self.run_workflow(run_args)
        # then
        rai_config = self.resource_manager.get_rai_config()
        rsp_json = workflow.rai.execute_relation_json(self.logger, rai_config, self.env_config, RESOURCES_TO_DELETE_REL)
        self.assertEqual(rsp_json, [{'partition': 2023090800001, 'relation': 'city_data'}])

    def test_scenario3_model_two_partitions_overriden_by_one_for_date_partitioned(self):
        # when
        self.create_workflow("./config/model/scenario3.json")
        run_args = ["--start-date", "20230908", "--end-date", "20230909"]
        # first run
        # copy data for scenario 3
        data_folder = "/city"
        shutil.copytree(f"{self.dev_data_dir}{data_folder}", f"{self.temp_folder}{data_folder}")
        self.run_workflow(run_args)
        # second run
        # rename files to simulate data refresh
        os.rename(f"{self.temp_folder}{data_folder}/data_dt=20230908/part-1-3d1ec0b0-ebfd-a773-71d7-f71f42a2f066.csv",
                  f"{self.temp_folder}{data_folder}/data_dt=20230908/part-1-{uuid.uuid4()}.csv")
        os.remove(f"{self.temp_folder}{data_folder}/data_dt=20230908/part-2-3d9ec0b0-ebfd-a773-71d7-f71f42a2f066.csv")
        self.run_workflow(run_args)
        # then
        rai_config = self.resource_manager.get_rai_config()
        rsp_json = workflow.rai.execute_relation_json(self.logger, rai_config, self.env_config, RESOURCES_TO_DELETE_REL)
        self.assertEqual(rsp_json, [{'partition': 2023090800001, 'relation': 'city_data'},
                                    {'partition': 2023090800002, 'relation': 'city_data'}])

    def test_scenario4_model_reimport_2_partitions_data_with_1(self):
        # when
        self.create_workflow("./config/model/scenario4.json")
        run_args = ["--start-date", "20230908", "--end-date", "20230908"]
        # first run
        # copy data for scenario 4
        data_folder = "/city"
        shutil.copytree(f"{self.dev_data_dir}{data_folder}", f"{self.temp_folder}{data_folder}")
        self.run_workflow(run_args)
        # second run
        # replace files to simulate data refresh
        shutil.rmtree(f"{self.temp_folder}{data_folder}/data_dt=20230908")
        shutil.copytree(f"{self.dev_data_dir}{data_folder}/data_dt=20230909",
                        f"{self.temp_folder}{data_folder}/data_dt=20230908")
        self.run_workflow(run_args)
        # then
        self.assert_output_dir_files(self.test_scenario4_model_reimport_2_partitions_data_with_1.__name__)

    def test_scenario5_model_single_partition_change(self):
        # when
        self.create_workflow("./config/model/scenario5.json")
        run_args = ["--start-date", "20230908", "--end-date", "20230909"]
        # first run
        # copy data for scenario 5
        data_folder = "/product"
        shutil.copytree(f"{self.dev_data_dir}{data_folder}", f"{self.temp_folder}{data_folder}")
        self.run_workflow(run_args)
        # second run
        # rename files to simulate data refresh
        os.rename(f"{self.temp_folder}{data_folder}/part-1-3q1ec0b0-ebfd-a773-71d7-f71f42a2f066.csv",
                  f"{self.temp_folder}{data_folder}/part-1-{uuid.uuid4()}.csv")
        self.run_workflow(run_args)
        # then
        rai_config = self.resource_manager.get_rai_config()
        rsp_json = workflow.rai.execute_relation_json(self.logger, rai_config, self.env_config, RESOURCES_TO_DELETE_REL)
        self.assertEqual(rsp_json, [{'partition': 1, 'relation': 'product_data'}])

    def test_scenario6_model_two_partitions_overriden_by_one(self):
        # when
        self.create_workflow("./config/model/scenario6.json")
        # first run
        # copy data for scenario 6
        data_folder = "/product"
        shutil.copytree(f"{self.dev_data_dir}{data_folder}", f"{self.temp_folder}{data_folder}")
        self.run_workflow()
        # second run
        # rename files to simulate data refresh
        os.rename(f"{self.temp_folder}{data_folder}/part-1-3q1ec0b0-ebfd-a773-71d7-f71f42a2f066.csv",
                  f"{self.temp_folder}{data_folder}/part-1-{uuid.uuid4()}.csv")
        os.remove(f"{self.temp_folder}{data_folder}/part-2-3w1ec0b0-ebfd-a773-71d7-f71f42a2f066.csv")
        self.run_workflow()
        # then
        rai_config = self.resource_manager.get_rai_config()
        rsp_json = workflow.rai.execute_relation_json(self.logger, rai_config, self.env_config, RESOURCES_TO_DELETE_REL)
        self.assertEqual(rsp_json, [{'relation': 'product_data'}])

    # def test_scenario7_model_1_day_snapshot_2_day_declared_1_day_out_of_range(self):
    #     # when
    #     test_args = ["--batch-config", "./config/model/scenario7.json"]
    #     rsp = call(self.cmd_with_common_arguments + test_args + ["--end-date", "20220102", "--drop-db"])
    #     # then
    #     self.assertNotEqual(rsp, 1)
    #     # and when
    #     rsp = call(self.cmd_with_common_arguments + test_args + ["--end-date", "20220104"])
    #     # then
    #     self.assertNotEqual(rsp, 1)
    #     rai_config = self.resource_manager.get_rai_config()
    #     rsp_json = workflow.rai.execute_relation_json(self.logger, rai_config, self.env_config, RESOURCES_TO_DELETE_REL)
    #     self.assertEqual(rsp_json, [{'relation': 'device_seen_snapshot'}])
    #
    # def test_scenario7_model_1_day_snapshot_1_day_declared_1_day_out_of_range(self):
    #     # when
    #     test_args = ["--batch-config", "./config/model/scenario7.json"]
    #     rsp = call(self.cmd_with_common_arguments + test_args + ["--start-date", "20220103", "--end-date", "20220104",
    #                                                              "--drop-db"])
    #     # then
    #     self.assertNotEqual(rsp, 1)
    #     # and when
    #     rsp = call(self.cmd_with_common_arguments + test_args + ["--end-date", "20220105"])
    #     # then
    #     self.assertNotEqual(rsp, 1)
    #     rai_config = self.resource_manager.get_rai_config()
    #     rsp_json = workflow.rai.execute_relation_json(self.logger, rai_config, self.env_config, RESOURCES_TO_DELETE_REL)
    #     self.assertEqual(rsp_json, {})
    #
    # def test_scenario7_model_1_day_snapshot_1_day_declared_0_days_out_of_range(self):
    #     # when
    #     test_args = ["--batch-config", "./config/model/scenario7.json"]
    #     rsp = call(self.cmd_with_common_arguments + test_args + ["--end-date", "20220105", "--drop-db"])
    #     # then
    #     self.assertNotEqual(rsp, 1)
    #     # and when
    #     rsp = call(self.cmd_with_common_arguments + test_args + ["--end-date", "20220105"])
    #     # then
    #     self.assertNotEqual(rsp, 1)
    #     rai_config = self.resource_manager.get_rai_config()
    #     rsp_json = workflow.rai.execute_relation_json(self.logger, rai_config, self.env_config, RESOURCES_TO_DELETE_REL)
    #     self.assertEqual(rsp_json, {})
    #
    # def test_scenario8_model_2_day_snapshot_1_day_declared_1_days_out_of_range(self):
    #     # when
    #     test_args = ["--batch-config", "./config/model/scenario8.json"]
    #     rsp = call(self.cmd_with_common_arguments + test_args + ["--end-date", "20220103", "--drop-db"])
    #     # then
    #     self.assertNotEqual(rsp, 1)
    #     # and when
    #     rsp = call(self.cmd_with_common_arguments + test_args + ["--start-date", "20220104", "--end-date", "20220105"])
    #     # then
    #     self.assertNotEqual(rsp, 1)
    #     rai_config = self.resource_manager.get_rai_config()
    #     rsp_json = workflow.rai.execute_relation_json(self.logger, rai_config, self.env_config, RESOURCES_TO_DELETE_REL)
    #     self.assertEqual(rsp_json, [{'relation': 'device_seen_snapshot'}])

    # def test_scenario9_model_do_not_inflate_paths_when_snapshot_is_valid(self):
    #     # when
    #     test_args = ["--batch-config", "./config/model/scenario9.json"]
    #     rsp = call(self.cmd_with_common_arguments + test_args + ["--end-date", "20220102", "--drop-db"])
    #     # then
    #     self.assertNotEqual(rsp, 1)
    #     # and when
    #     rsp = call(self.cmd_with_common_arguments + test_args + ["--end-date", "20220103"])
    #     # then
    #     self.assertNotEqual(rsp, 1)
    #     rai_config = self.resource_manager.get_rai_config()
    #     query = workflow.query.get_snapshot_expiration_date("device_seen_snapshot", DATE_FORMAT)
    #     expiration_date_str = workflow.rai.execute_query_take_single(self.logger, rai_config, self.env_config, query)
    #     self.assertEqual(expiration_date_str, "20220104")

    # def test_scenario9_model_do_not_inflate_paths_when_snapshot_expired(self):
    #     # when
    #     test_args = ["--batch-config", "./config/model/scenario9.json"]
    #     rsp = call(self.cmd_with_common_arguments + test_args + ["--end-date", "20220102", "--drop-db"])
    #     # then
    #     self.assertNotEqual(rsp, 1)
    #     # and when
    #     rsp = call(self.cmd_with_common_arguments + test_args + ["--end-date", "20220105"])
    #     # then
    #     self.assertNotEqual(rsp, 1)
    #     rai_config = self.resource_manager.get_rai_config()
    #     query = workflow.query.get_snapshot_expiration_date("device_seen_snapshot", DATE_FORMAT)
    #     expiration_date_str = workflow.rai.execute_query_take_single(self.logger, rai_config, self.env_config, query)
    #     self.assertEqual(expiration_date_str, "20220106")

    def create_workflow(self, scenario):
        args = ["--batch-config", scenario,
                "--action", "init",
                "--drop-db"]
        rsp = call(self.cmd_with_common_arguments + args)
        self.assertNotEqual(rsp, 1)

    def run_workflow(self, args: list[str] = None):
        run_args = ["--rel-config-dir", "./rel",
                    "--action", "run"]
        if args is not None:
            run_args = run_args + args
        rsp = call(self.cmd_with_common_arguments + run_args)
        self.assertNotEqual(rsp, 1)

    @classmethod
    def setUpClass(cls) -> None:
        # Make sure output folder is empty since the folder share across repository. Remove README.md, other files left.
        cls.cleanup_output()
        cls.logger = logging.getLogger("cli-e2e-test")
        with open(cls.env_config_path, "rb") as fp:
            loader_config = tomli.load(fp)
        cls.env_config = workflow.common.EnvConfig.from_env_vars(loader_config)
        cls.resource_manager = workflow.manager.ResourceManager.init(cls.logger, cls.resource_name, cls.resource_name,
                                                                     cls.env_config)
        cls.logger.setLevel(logging.INFO)
        cls.logger.addHandler(logging.StreamHandler())
        cls.resource_manager.add_engine(size="S")

    def tearDown(self):
        self.cleanup_output()
        if os.path.exists(self.temp_folder):
            shutil.rmtree(self.temp_folder)

    @classmethod
    def tearDownClass(cls) -> None:
        workflow.semantic_layer_service.shutdown(cls.logger, cls.env_config, cls.resource_manager)
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

    @classmethod
    def cleanup_output(cls):
        for filename in os.listdir(cls.output):
            file_path = os.path.join(cls.output, filename)
            if os.path.isfile(file_path):
                os.remove(file_path)
