import unicodedata


class Cleaning:

    @staticmethod
    def clean_subject_name(raw_subject_name: str) -> str:
        cleaned_subject_name = (unicodedata.normalize('NFKD', raw_subject_name)
                                           .encode('ASCII', 'ignore')
                                           .decode('utf-8'))  # removes accents

        cleaned_subject_name = (cleaned_subject_name.strip()
                                                    .lower()
                                                    .replace('.', ''))
        return cleaned_subject_name
