import re
from argparse import ArgumentParser, Namespace, BooleanOptionalAction
from cli.logger import LogRotationOption

prohibited_symbols_in_file_name = re.compile(r'[\\/:*?"<>|]')


def parse() -> Namespace:
    parser = ArgumentParser()
    parser.add_argument(
        "--batch-config",
        help="Relative path to batch configuration json",
        required=True,
        type=str
    )
    parser.add_argument(
        "--batch-config-name",
        help="The name of the batch configuration json",
        required=False,
        type=str,
        default="default"
    )
    parser.add_argument(
        "--database",
        help="RAI database",
        required=True,
        type=str
    )
    parser.add_argument(
        "--source-database",
        help="RAI database for clone",
        required=False,
        type=str
    )
    parser.add_argument(
        "--engine",
        help="RAI engine",
        required=True,
        type=str
    )
    parser.add_argument(
        "--start-date", help="Start date for model data. Format: 'YYYYmmdd'",
        required=False,
        type=str
    )
    parser.add_argument(
        "--end-date", help="End date for model data. Format: 'YYYYmmdd'",
        required=False,
        type=str
    )
    parser.add_argument(
        "--force-reimport-not-chunk-partitioned",
        help="Force reimport of sources which are NOT chunk-partitioned. If it's a date-partitioned source, it will be "
             "re-imported with in `--start-date` & `--end-date` range.",
        action=BooleanOptionalAction,
        default=False
    )
    parser.add_argument(
        "--force-reimport",
        help="Force reimport of sources which are date-partitioned (both chunk and NOT chunk-partitioned) with in "
             "`--start-date` & `--end-date` range and all sources which are NOT date-partitioned.",
        action=BooleanOptionalAction,
        default=False
    )
    parser.add_argument(
        "--rel-config-dir", help="Directory containing rel config files to install",
        required=False,
        default="../rel",
        type=str
    )
    parser.add_argument(
        "--env-config",
        help="Relative path to toml file containing environment specific RAI settings",
        required=False,
        default="../config/loader.toml",
        type=str
    )
    parser.add_argument(
        "--collapse-partitions-on-load",
        help="When loading each multi-part source, load all partitions (and shards) in one transaction",
        action=BooleanOptionalAction,
        default=True
    )
    parser.add_argument(
        "--log-level",
        help="Set log level",
        required=False,
        default="INFO",
        type=str
    )
    parser.add_argument(
        "--log-rotation",
        help="Log rotation option",
        choices=[LogRotationOption.DATE, LogRotationOption.SIZE],
        required=False,
        type=LogRotationOption,
        default=LogRotationOption.DATE
    )
    parser.add_argument(
        "--log-file-size",
        help="Rotation log file size in Mb. RWM rotates log file when it reaches this size",
        required=False,
        type=int,
        default=5  # 5 Mb
    )
    parser.add_argument(
        "--log-file-name",
        help="Log file name",
        required=False,
        type=str,
        default="rwm"
    )
    parser.add_argument(
        "--drop-db",
        help="Drop RAI database before run, or not",
        action=BooleanOptionalAction,
        default=False
    )
    parser.add_argument(
        "--engine-size",
        help="Size of RAI engine",
        required=False,
        default="XS",
        type=str
    )
    parser.add_argument(
        "--cleanup-resources",
        help="Remove RAI engine and database after run or not",
        action=BooleanOptionalAction,
        default=False
    )
    parser.add_argument(
        "--cleanup-db",
        help="Remove RAI database after run or not",
        action=BooleanOptionalAction,
        default=False
    )
    parser.add_argument(
        "--cleanup-engine",
        help="Remove RAI engine after run or not",
        action=BooleanOptionalAction,
        default=False
    )
    parser.add_argument(
        "--disable-ivm",
        help="Disable IVM for RAI database",
        action=BooleanOptionalAction,
        default=True
    )
    parser.add_argument(
        "--recover",
        help="Recover a batch run starting from a FAILED step",
        action=BooleanOptionalAction,
        default=False
    )
    parser.add_argument(
        "--recover-step",
        help="Recover a batch run starting from specified step",
        required=False,
        type=str
    )
    parser.add_argument(
        "--selected-steps",
        help="Steps from batch config to run",
        nargs='+',
        required=False,
        type=str
    )
    parser.add_argument(
        "--rai-sdk-http-retries",
        help="Parameter to set http retries for rai SDK",
        required=False,
        default=3,
        type=int
    )
    parser.add_argument(
        "--step-timeout",
        help="Parameter to set timeouts for steps",
        required=False,
        type=str
    )
    args = parser.parse_args()
    # Validation
    if 'selected_steps' in vars(args) and args.selected_steps and 'recover_step' in vars(args) and args.recover_step:
        parser.error("`--recover-step` can't be used when selected-steps are specified.")
    if 'recover' in vars(args) and args.recover and 'recover_step' in vars(args) and args.recover_step:
        parser.error("`--recover` and `--recover-step` options are mutually exclusive. You must choose only 1 option.")
    if 'step_timeout' in vars(args):
        try:
            args.step_timeout_dict = parse_string_int_key_value_argument(args.step_timeout)
        except ValueError:
            parser.error("`--step-timeout` should be key value pairs separated by comma. Value must have `int` type. "
                         "Example: `--step-timeout \"step1=10,step2=20`\"")
    if 'log_file_name' in vars(args):
        if prohibited_symbols_in_file_name.search(args.log_file_name):
            parser.error(f"`--log-file-name` contains prohibited symbols: {prohibited_symbols_in_file_name.pattern}")
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
