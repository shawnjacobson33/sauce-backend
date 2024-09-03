
class DataCleaner:

    @staticmethod
    def clean_league(league: str):
        leagues_mapping = {
            # BOOM FANTASY
            'NCAAFB': 'NCAAF',  # and HotStreak
            'MTEN': 'TENNIS',
            'WTEN': 'TENNIS',
            'HDOG': 'HOTDOG',
            # Dabble
            'Tennis W': 'TENNIS',
            'Tennis M': 'TENNIS',
            # HotStreak
            'Valorant': 'VAL',
            'CCT Europe': 'CS',
            'ESL Pro League': 'CS',
            'Thunderpick World Championship': 'CS',
            'YaLLa Compass': 'CS',
            'CCT South America': 'CS',
            'Elisa Invitational': 'CS',
            'ESL Challenger League': 'CS',
            'CS2': 'CS',
            'CSGO': 'CS',
            # OddsShopper
            'WTA': 'TENNIS',
            'ATP': 'TENNIS',
            # ParlayPlay
            'CFB': 'NCAAF',
            # PrizePicks
            'MLBLIVE': 'MLB',
            'UFC': 'MMA',
            'EPL': 'SOCCER',
            'MLS': 'SOCCER',


        }

        return leagues_mapping.get(league, league)
