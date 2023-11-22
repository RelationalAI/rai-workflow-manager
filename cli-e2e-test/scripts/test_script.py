import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)
stream_handler = logging.StreamHandler()
formatter = logging.Formatter('[%(asctime)s.%(msecs)03d][%(levelname)s][%(name)s] %(message)s', '%Y-%m-%d %H:%M:%S')
stream_handler.setFormatter(formatter)
logger.addHandler(stream_handler)
my_logger = logger.getChild("my_test_logger")
my_logger.info("Executed python and print message using logger")
