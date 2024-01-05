from workflow.common import BaseEnum


class CliAction(str, BaseEnum):
    RUN = 'run'
    INIT = 'init'
