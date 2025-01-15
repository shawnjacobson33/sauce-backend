# Description: Configurations for the data collection service.
_CONFIGS = {
    'standardization': {
        'invalid_markets': {'First Basket', 'Top Point Scorer'}
        # Todo: need an invalid leagues list and then create a standardize league method
    },
    'betting_lines': {
        'reset': True,
        'throttle': 720,
        'data_collection': {
            'valid_leagues': {'NBA', 'NCAAM'},
        },
        'data_processing': {
            'ev_formulas': {
                'main_markets': '????',
                'secondary_markets': {
                    'name': 'sully'
                }
            }
        }
    },
    'rosters': {
        'reset': True,
        'throttle': 60*60*24,
        'data_collection': {
            'valid_leagues': {'NBA', 'NCAAM'},
            'ncaa_conferences_to_collect_from': {
                'ACC', 'Big East', 'Big Ten', 'Big 12', 'Ivy', 'Mid American', 'Mountain West', 'SEC', 'West Coast'
            }
        },
    },
    'games': {
        'reset': True,
        'throttle': 720,
        'data_collection': {
            'valid_leagues': {'NBA', 'NCAAM'},
        }
    },
    'box_scores': {
        'reset': True,
        'throttle': 60,
        'data_collection': {
            'valid_leagues': {'NBA', 'NCAAM'},
        }
    }
}


def load_configs(pipeline_sub_process: str = None) -> dict:
    if not pipeline_sub_process:
        return _CONFIGS

    if specific_configs := _CONFIGS.get(pipeline_sub_process):
        return specific_configs

    raise ValueError(f"Configs for pipeline_sub_process '{pipeline_sub_process}' not found.")