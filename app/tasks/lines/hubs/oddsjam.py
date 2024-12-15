from app import utils as ln_utils



class OddsJam(ln_utils.LinesRetriever):
    def __init__(self, batch_id: str, lines_hub: ln_utils.LinesSource):
        # call parent class Plug
        super().__init__(batch_id, lines_hub)