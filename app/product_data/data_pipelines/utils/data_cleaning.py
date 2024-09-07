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
            'EUROGOLF': 'GOLF'


        }

        return leagues_mapping.get(league, league)

    @staticmethod
    def clean_subject(subject: str):
        # Define suffixes in a list with regex patterns to ensure they are standalone with spaces
        suffixes = [
            r' Jr\.?$', r' Sr\.?$', r' II+$', r' III+$', r' IV$', r' V$'
        ]

        # Create a regex pattern to match any of the suffixes
        pattern = '|'.join(suffixes)

        # Replace the found suffixes with nothing (remove them)
        # Ensuring the suffix is either followed by the end of the string or has space around it
        cleaned_subject = re.sub(pattern, '', subject).strip()

        # Return the cleaned subject
        return cleaned_subject
