import logging
import dataclasses
from logging.handlers import TimedRotatingFileHandler, RotatingFileHandler
from workflow.common import BaseEnum


class LogRotationOption(str, BaseEnum):
    DATE = 'date'
    SIZE = 'size'


@dataclasses.dataclass
class LogConfiguration:
    level: int = logging.INFO
    rotation: LogRotationOption = LogRotationOption.DATE
    log_file_size: int = 5
    log_file_name: str = "rwm"


def configure(config: LogConfiguration) -> logging.Logger:
    # override default logging level for azure
    logger = logging.getLogger('azure.core')
    logger.setLevel(logging.ERROR)
    logger = logging.getLogger('snowflake')
    logger.setLevel(logging.ERROR)

    logger = logging.getLogger()
    logger.setLevel(config.level)
    formatter = logging.Formatter('[%(asctime)s.%(msecs)03d][%(levelname)s][%(name)s] %(message)s', '%Y-%m-%d %H:%M:%S')

    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)

    full_log_file_name = f"{config.log_file_name}.log"

    if config.rotation == LogRotationOption.SIZE:
        max_log_size = config.log_file_size * 1024 * 1024  # covert to Mb
        size_rotation_handler = RotatingFileHandler(full_log_file_name, maxBytes=max_log_size, backupCount=30,
                                                    encoding='utf-8')
        size_rotation_handler.setFormatter(formatter)
        logger.addHandler(size_rotation_handler)
        logger.info("Log rotation by size is enabled")
    elif config.rotation == LogRotationOption.DATE:
        time_rotation_handler = TimedRotatingFileHandler(full_log_file_name, when='midnight', interval=1,
                                                         backupCount=30, encoding='utf-8')
        time_rotation_handler.setFormatter(formatter)
        logger.addHandler(time_rotation_handler)
        logger.info("Log rotation by date is enabled")

    return logger.getChild("cli")
