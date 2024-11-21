import re
import unicodedata


SUBJECT_MAP = {
    'Basketball': {
        ''
    }
}
SUFFIXES = [
    'Jr.', 'jr.', 'jr', 'Jr', 'Sr.', 'sr.', 'sr', 'Sr', 'III', 'II', 'IV', 'V'
]


def normalize_accented_letters(subject_name: str):
    # map any accented letters to regular ASCII characters
    return unicodedata.normalize('NFKD', subject_name).encode('ASCII', 'ignore').decode('utf-8')


def remove_suffixes(subject_name: str):
    # for each suffix
    for suffix in SUFFIXES:
        # remove the suffix if it exists
        subject_name = subject_name.replace(f' {suffix}', '')

    # return the cleaned subject
    return subject_name


def clean_subject(subject_name: str) -> str:
    # map any accented letters to their nearest ASCII equivalent
    cleaned_subject_name = normalize_accented_letters(subject_name)
    # remove any suffixes
    cleaned_subject_name = remove_suffixes(cleaned_subject_name)
    # remove any punctuation (T.J., Wan'Dale, Ray-Ray)
    cleaned_subject_name = cleaned_subject_name.replace('.', '').replace("'", '').replace('-', ' ')
    # remove any digits or extra spaces
    cleaned_subject_name = re.sub(r'\s+', ' ', cleaned_subject_name)  # Replace any double or more spaces with singles
    cleaned_subject_name = re.sub(r'\d', '', cleaned_subject_name).strip()  # Remove digits and extra whitespace
    # return the cleaned subject name, uppercased
    return cleaned_subject_name.title()