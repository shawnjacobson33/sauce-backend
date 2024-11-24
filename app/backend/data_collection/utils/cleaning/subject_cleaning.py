import re
import unicodedata


SUBJECT_MAP = {
    'NBA': {
        'Cameron Thomas': 'Cam Thomas',
        'Cam Johnson': 'Cameron Johnson',
        'Alex Sarr': 'Alexandre Sarr',
        'Carlton Carrington': 'Bub Carrington'
    },
    'NFL': {
        'Chigoziem Okonkwo': 'Chig Okonkwo',
        'Josh Palmer': 'Joshua Palmer',
        'Christopher Brooks': 'Chris Brooks',
        'Andrew Ogletree': 'Drew Ogletree',
        'Joshua Metellus': 'Josh Metellus',
        'Jartavius Martin': 'Quan Martin',
        'Patrick Surtain II': 'Pat Surtain II',
        'Tariq Woolen': 'Riq Woolen',
        'Kamren Curl': 'Kam Curl',
        'Decobie Durant': 'Cobie Durant'
    },
    'NHL': {
        'Mitchell Marner': 'Mitch Marner',
        'Christopher Tanev': 'Chris Tanev'
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


def clean_subject(subject_name: str, league: str) -> str:
    if f_subject_map := SUBJECT_MAP.get(league):
        return f_subject_map.get(subject_name, subject_name)

    # map any accented letters to their nearest ASCII equivalent
    c_name = normalize_accented_letters(subject_name)
    # remove any suffixes
    c_name = remove_suffixes(c_name)
    # remove any punctuation (T.J., Wan'Dale, Ray-Ray)
    c_name = c_name.replace('.', '').replace("'", '').replace('-', ' ')
    # remove any digits or extra spaces
    c_name = re.sub(r'\s+', ' ', c_name)  # Replace any double or more spaces with singles
    c_name = re.sub(r'\d', '', c_name).strip()  # Remove digits and extra whitespace
    # return the cleaned subject name, uppercased
    # TODO: Map to the correct format
    return c_name.title()