class Storing:
    """
    A utility class for generating keys for subjects and betting lines.
    """

    @staticmethod
    def get_subject_key(league: str, subject_name: str, subject_attribute: str = None) -> str:
        """
        Generates a key for a subject based on the league, subject name, and optional subject attribute.

        Args:
            league (str): The league to which the subject belongs.
            subject_name (str): The name of the subject.
            subject_attribute (str, optional): An optional attribute of the subject. Defaults to None.

        Returns:
            str: The generated subject key.
        """
        if subject_attribute:
            return f"{league}:{subject_attribute}:{subject_name}"

        return f"{league}:{subject_name}"

    @staticmethod
    def get_betting_line_key(line: dict) -> str:
        """
        Generates a key for a betting line based on the bookmaker, league, market, subject, and label.

        Args:
            line (dict): A dictionary containing the betting line details.

        Returns:
            str: The generated betting line key.
        """
        try:
            return f"{line['bookmaker']}:{line['league']}:{line['market']}:{line['subject']}:{line['label']}"

        except Exception as e:
            raise ValueError(f"Failed to generate betting line key: {e}")
