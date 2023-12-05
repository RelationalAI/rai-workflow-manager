class RAIException(Exception):
    """RAI related exception"""

    def __init__(self, msg):
        super().__init__(msg)


class ConcurrentWriteAttemptException(RAIException):
    """Exception raised when RWM tries to submit write txn to RAI engine which already has RUNNING write txn"""

    def __init__(self, engine_name):
        super().__init__(f"'{engine_name}' has already running write transaction")


class RetryException(Exception):
    """Exception raised when retry limits are reached (timeout, max retries, etc.)"""

    def __init__(self, msg):
        super().__init__(msg)


class StepTimeOutException(Exception):
    """Exception raised when step execution time exceeds timeout"""

    def __init__(self, msg):
        super().__init__(msg)


class CommandExecutionException(Exception):
    """Exception raised when ExecuteCommand step failed"""

    def __init__(self, command, status):
        super().__init__(f"Cli command:\n{command}\nfailed with exit status '{status}'")


class RestClientException(Exception):
    """Exception raised in case of errors in RestClient"""

    def __init__(self, method, url, error):
        super().__init__(f"Rest request '{method}: {url}' failed. Exception: {error}")
