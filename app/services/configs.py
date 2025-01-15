# Description: Configurations for the data collection service.
_CONFIGS = {
    'betting_lines': {
        'reset': False,
        'data_collection': {
            'valid_leagues': {'NBA', 'NCAAM'},
            'invalid_markets': {'First Basket', 'Top Point Scorer'}
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
        'reset': False,
        'data_collection': {
            'valid_leagues': {'NBA', 'NCAAM'},
            'ncaa_conferences_to_collect_from': {
                'ACC', 'Big East', 'Big Ten', 'Big 12', 'Ivy', 'Mid American', 'Mountain West', 'SEC', 'West Coast'
            }
        },
    },
    'games': {
        'reset': False,
        'data_collection': {
            'valid_leagues': {'NBA', 'NCAAM'},
        }
    },
    'box_scores': {
        'reset': False,
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