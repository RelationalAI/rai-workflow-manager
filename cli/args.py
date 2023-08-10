from argparse import ArgumentParser, Namespace, BooleanOptionalAction

import workflow.executor


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
        help="Name of to batch configuration json",
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
        "--engine",
        help="RAI engine",
        required=True,
        type=str
    )
    parser.add_argument(
        "--run-mode",
        help="Type of run mode",
        required=True,
        type=workflow.executor.WorkflowRunMode,
        choices=list(workflow.executor.WorkflowRunMode),
    )
    parser.add_argument(
        "--start-date", help="Start date for model data. Format: 'YYYYmmdd'",
        required=False,
        type=str
    )
    parser.add_argument(
        "--end-date", help="End date for model data. Format: 'YYYYmmdd'",
        required=True,
        type=str
    )
    parser.add_argument(
        "--dev-data-dir", help="Directory containing dev data",
        required=False,
        default="../data",
        type=str
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
        required=False,
        default=True,
        type=bool
    )
    parser.add_argument(
        "--output-root",
        help="Output folder path for dev mode",
        required=False,
        default="../../output",
        type=str
    )
    parser.add_argument(
        "--log-level",
        help="Set log level",
        required=False,
        default="INFO",
        type=str
    )
    parser.add_argument(
        "--drop-db",
        help="Drop RAI database before run, or not",
        required=False,
        default=True,
        type=bool
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
        required=False,
        default=False,
        type=bool
    )
    parser.add_argument(
        "--disable-ivm",
        help="Disable IVM for RAI database",
        required=False,
        default=True,
        type=bool
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
        default=False,
        type=str
    )
    parser.add_argument(
        "--rai-sdk-http-retries",
        help="Parameter to set http retries for rai SDK",
        required=False,
        default=3,
        type=int
    )
    args = parser.parse_args()
    # Validation
    if 'recover' in vars(args) and args.recover and 'recover_step' in vars(args) and args.recover_step:
        parser.error("`--recover` and `--recover-step` options are mutually exclusive. Use must choose only 1 option.")
    return args
