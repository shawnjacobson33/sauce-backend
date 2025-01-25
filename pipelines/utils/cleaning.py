import unicodedata

# List of suffixes to be removed from subject names
SUFFIXES = [
    'Jr.', 'jr.', 'jr', 'Jr', 'Sr.', 'sr.', 'sr', 'Sr', 'III', 'II', 'IV', 'V', 'iii', 'ii', 'iv', 'v',
]


class Cleaning:
    """
    A utility class for cleaning subject names.
    """

    @staticmethod
    def clean_subject_name(raw_subject_name: str) -> str:
        """
        Cleans the given raw subject name by normalizing, removing accents, suffixes, and unnecessary characters.

        Args:
            raw_subject_name (str): The raw subject name to be cleaned.

        Returns:
            str: The cleaned subject name.
        """
        # Normalize the subject name to remove accents
        cleaned_subject_name = (unicodedata.normalize('NFKD', raw_subject_name)
                                           .encode('ASCII', 'ignore')
                                           .decode('utf-8'))  # removes accents

        # Remove suffixes from the subject name
        for suffix in SUFFIXES:
            cleaned_subject_name = cleaned_subject_name.replace(f' {suffix}', '')

        # Strip, convert to lowercase, and remove periods from the subject name
        cleaned_subject_name = (cleaned_subject_name.strip()
                                                    .lower()
                                                    .replace('.', '')
                                                    .replace('-', '')
                                                    .replace("'", ''))
        return cleaned_subject_name