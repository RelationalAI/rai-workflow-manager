import re
from argparse import ArgumentParser, Namespace, BooleanOptionalAction
from cli.logger import LogRotationOption

prohibited_symbols_in_file_name = re.compile(r'[\\/:*?"<>|]')


def parse() -> Namespace:
    common_args = {
        "--action": {
            "help": "Cli action",
            "choices": ["run", "init"],
            "required": True,
            "type": str
        },
        "--database": {
            "help": "RAI database",
            "required": True,
            "type": str
        },
        "--engine": {
            "help": "RAI engine",
            "required": True,
            "type": str
        },
        "--cleanup-resources": {
            "help": "Remove RAI engine and database after run or not",
            "action": BooleanOptionalAction,
            "default": False
        },
        "--cleanup-engine": {
            "help": "Remove RAI engine after action's execution or not",
            "action": BooleanOptionalAction,
            "default": False
        },
        "--cleanup-db": {
            "help": "Remove RAI database after run or not",
            "action": BooleanOptionalAction,
            "default": False
        },
        "--batch-config-name": {
            "help": "The name of the batch configuration json",
            "required": False,
            "type": str,
            "default": "default"
        },
        "--env-config": {
            "help": "Relative path to toml file containing environment specific RAI settings",
            "required": False,
            "default": "../config/loader.toml",
            "type": str
        },
        "--log-level": {
            "help": "Set log level",
            "required": False,
            "default": "INFO",
            "type": str
        },
        "--log-rotation": {
            "help": "Log rotation option",
            "choices": [LogRotationOption.DATE, LogRotationOption.SIZE],
            "required": False,
            "type": LogRotationOption,
            "default": LogRotationOption.DATE
        },
        "--log-file-size": {
            "help": "Rotation log file size in Mb. RWM rotates log file when it reaches this size",
            "required": False,
            "type": int,
            "default": 5  # 5 Mb
        },
        "--log-file-name": {
            "help": "Log file name",
            "required": False,
            "type": str,
            "default": "rwm"
        },
        "--engine-size": {
            "help": "Size of RAI engine",
            "required": False,
            "default": "XS",
            "type": str
        },
        "--rai-sdk-http-retries": {
            "help": "Parameter to set http retries for rai SDK",
            "required": False,
            "default": 3,
            "type": int
        }
    }
    init_args = {
        "--batch-config": {
            "help": "Relative path to batch configuration json",
            "required": True,
            "type": str
        },
        "--source-database": {
            "help": "RAI database for clone",
            "required": False,
            "type": str
        },
        "--drop-db": {
            "help": "Drop RAI database before run, or not",
            "action": BooleanOptionalAction,
            "default": False
        },
        "--disable-ivm": {
            "help": "Disable IVM for RAI database",
            "action": BooleanOptionalAction,
            "default": True
        }
    }
    run_args = {
        "--start-date": {
            "help": "Start date for model data. Format: 'YYYYmmdd'",
            "required": False,
            "type": str
        },
        "--end-date": {
            "help": "End date for model data. Format: 'YYYYmmdd'",
            "required": False,
            "type": str
        },
        "--force-reimport-not-chunk-partitioned": {
            "help": "Force reimport of sources which are NOT chunk-partitioned. If it's a date-partitioned source, it "
                    "will be re-imported with in `--start-date` & `--end-date` range.",
            "action": BooleanOptionalAction,
            "default": False
        },
        "--force-reimport": {
            "help": "Force reimport of sources which are date-partitioned (both chunk and NOT chunk-partitioned) with "
                    "in `--start-date` & `--end-date` range and all sources which are NOT date-partitioned.",
            "action": BooleanOptionalAction,
            "default": False
        },
        "--rel-config-dir": {
            "help": "Directory containing rel config files to install",
            "required": False,
            "default": "../rel",
            "type": str
        },
        "--collapse-partitions-on-load": {
            "help": "When loading each multi-part source, load all partitions (and shards) in one transaction",
            "action": BooleanOptionalAction,
            "default": True
        },
        "--recover": {
            "help": "Recover a batch run starting from a FAILED step",
            "action": BooleanOptionalAction,
            "default": False
        },
        "--step-timeout": {
            "help": "Parameter to set timeouts for steps",
            "required": False,
            "type": str
        }
    }

    action_parser = ArgumentParser()
    for a in common_args:
        action_parser.add_argument(a, **common_args[a])
    action = action_parser.parse_known_args()[0].action

    parser = ArgumentParser()
    for a in common_args:
        parser.add_argument(a, **common_args[a])
    if action == "init":
        for a in init_args:
            parser.add_argument(a, **init_args[a])
    elif action == "run":
        for a in run_args:
            parser.add_argument(a, **run_args[a])
    args = parser.parse_args()
    # Validation
    if 'step_timeout' in vars(args):
        try:
            args.step_timeout_dict = parse_string_int_key_value_argument(args.step_timeout)
        except ValueError:
            parser.error(
                "`--step-timeout` should be key value pairs separated by comma. Value must have `int` type. "
                "Example: `--step-timeout \"step1=10,step2=20`\"")
    if 'log_file_name' in vars(args):
        if prohibited_symbols_in_file_name.search(args.log_file_name):
            parser.error(
                f"`--log-file-name` contains prohibited symbols: {prohibited_symbols_in_file_name.pattern}")
    return args


def parse_string_int_key_value_argument(argument: str) -> dict[str, int]:
    result = {}
    if argument:
        for pair in argument.split(','):
            pair.strip()
            if pair != '':
                key, value = pair.split('=')
                result[key.strip()] = int(value.strip())
    return result
