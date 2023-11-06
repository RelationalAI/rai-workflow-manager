import logging
from logging.handlers import TimedRotatingFileHandler, RotatingFileHandler
from workflow.common import BaseEnum

log_filename = 'rwm.log'


class LogRotationOption(str, BaseEnum):
    DATE = 'date'
    SIZE = 'size'


def configure(level=logging.INFO, rotation: LogRotationOption = LogRotationOption.DATE,
              log_file_size: int = 5) -> logging.Logger:
    # override default logging level for azure
    logger = logging.getLogger('azure.core')
    logger.setLevel(logging.ERROR)
    logger = logging.getLogger('snowflake')
    logger.setLevel(logging.ERROR)

    logger = logging.getLogger()
    logger.setLevel(level)
    formatter = logging.Formatter('[%(asctime)s.%(msecs)03d][%(levelname)s][%(name)s] %(message)s', '%Y-%m-%d %H:%M:%S')

    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)

    if rotation == LogRotationOption.SIZE:
        max_log_size = log_file_size * 1024 * 1024  # covert to Mb
        size_rotation_handler = RotatingFileHandler(log_filename, maxBytes=max_log_size, backupCount=30,
                                                    encoding='utf-8')
        size_rotation_handler.setFormatter(formatter)
        logger.addHandler(size_rotation_handler)
        logger.info("Log rotation by size is enabled")
    elif rotation == LogRotationOption.DATE:
        time_rotation_handler = TimedRotatingFileHandler(log_filename, when='midnight', interval=1, backupCount=30,
                                                         encoding='utf-8')
        time_rotation_handler.setFormatter(formatter)
        logger.addHandler(time_rotation_handler)
        logger.info("Log rotation by date is enabled")

    return logger.getChild("cli")
