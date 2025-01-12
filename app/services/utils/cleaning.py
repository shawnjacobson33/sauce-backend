import unicodedata


SUFFIXES = [
    'Jr.', 'jr.', 'jr', 'Jr', 'Sr.', 'sr.', 'sr', 'Sr', 'III', 'II', 'IV', 'V'
]


class Cleaning:

    @staticmethod
    def clean_subject_name(raw_subject_name: str) -> str:
        cleaned_subject_name = (unicodedata.normalize('NFKD', raw_subject_name)
                                           .encode('ASCII', 'ignore')
                                           .decode('utf-8'))  # removes accents

        for suffix in SUFFIXES:
            cleaned_subject_name = cleaned_subject_name.replace(f' {suffix}', '')

        cleaned_subject_name = (cleaned_subject_name.strip()
                                                    .lower()
                                                    .replace('.', ''))
        return cleaned_subject_name
