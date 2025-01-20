import modin.pandas as pd  # Todo: consider switch to dask for parallel processing

from pipelines.base.base_processor import BaseProcessor


class PlayerPropsProcessor(BaseProcessor):

    def __init__(self, player_prop_lines: pd.DataFrame, configs: dict):
        super().__init__('PlayerProps', configs, player_prop_lines)


