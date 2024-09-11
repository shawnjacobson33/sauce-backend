import re


class DataCleaner:

    @staticmethod
    def clean_league(league: str):
        leagues_mapping = {
            # BOOM FANTASY
            'NCAAFB': 'NCAAF',  # and HotStreak
            'MTEN': 'TENNIS',
            'WTEN': 'TENNIS',
            'HDOG': 'HOTDOG',
            'NASCARMANUAL': 'NASCAR',
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
            'CCT North America': 'CS',
            'Elisa Invitational': 'CS',
            'ESL Challenger League': 'CS',
            'LCK': 'LOL',
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
            'EUROGOLF': 'GOLF',
            # Draftkings Pick6
            'PGA TOUR': 'GOLF',


        }
        return leagues_mapping.get(league, league).upper()

    @staticmethod
    def clean_subject(subject: str):
        # Define suffixes in a list with regex patterns to ensure they are standalone with spaces
        suffixes = [
            'Jr.', 'jr.', 'jr', 'Jr', 'Sr.', 'sr.', 'sr', 'Sr', 'III', 'II', 'IV'
        ]
        for suffix in suffixes:
            subject = re.sub(fr' {suffix}$', '', subject)

        return subject
