import logging


def configure(level=logging.INFO) -> logging.Logger:
    # override default logging level for azure
    logger = logging.getLogger('azure.core')
    logger.setLevel(logging.ERROR)

    logger = logging.getLogger()
    logger.setLevel(level)
    handler = logging.StreamHandler()
    formatter = logging.Formatter('[%(asctime)s][%(levelname)s][%(name)s] %(message)s', "%H:%M:%S")
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    return logger.getChild("cli")
