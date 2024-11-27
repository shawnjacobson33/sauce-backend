POSITION_MAP = {
    'Forward': 'F',
    'Pitcher': 'P',
    'Defender': 'D',
    'Hitter': 'H',
    'Guard': 'G',
    'Forward-Center': 'F-C',
}


def clean_position(position: str) -> str:
    """Small formatting fixes for positions"""
    return POSITION_MAP.get(position, position)