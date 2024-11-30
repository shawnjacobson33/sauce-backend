from app.backend.data_collection.workers.lines import utils as ln_utils



class OddsJam(ln_utils.LinesRetriever):
    def __init__(self, lines_hub: ln_utils.LinesSource):
        # call parent class Plug
        super().__init__(lines_hub)