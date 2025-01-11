

class BaseCollector:
    def __init__(self, name: str):
        self.name = name

    def run_collector(self):
        raise NotImplementedError