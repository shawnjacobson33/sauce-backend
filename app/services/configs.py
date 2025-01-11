# Description: Configurations for the data collection service.
_CONFIGS = {
    'general': {
        'leagues_to_collect_from': { 'NBA', 'NCAAM' }
    },
    'betting_lines': {
        'ev_formulas': {
            'main_markets': '????',
            'secondary_markets': 'sully'
        }  # Todo: what is the algo for calculating expected value? Store different formulas in db.
    },
    'rosters': {
        'ncaa_conferences_to_collect_from': {
            'ACC', 'Big East', 'Big Ten', 'Big 12', 'Ivy', 'Mid American', 'Mountain West', 'SEC', 'West Coast'
        }
    }
}


def load_configs(domain: str) -> dict:
    if specific_configs := _CONFIGS.get(domain):
        return specific_configs

    raise ValueError(f"Configs for domain '{domain}' not found.")