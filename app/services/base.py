


class BasePipeline:

    def __init__(self, reset: bool = False):
        self.reset = reset
        self.times = {}

    def run_pipeline(self):
        raise NotImplementedError
