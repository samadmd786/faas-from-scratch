from utils.serialize import serialize, deserialize


class WorkerFailure(Exception):
    def __init__(self, message="Worker failed or timed out"):
        super().__init__(message)
        self.message = message
