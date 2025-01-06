from datetime import datetime
from typing import Iterable

import pandas as pd
import pytest

from app.cache import main


@pytest.fixture
def setup():
    betting_lines = [
        {
            'timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            'bookmaker': 'PrizePicks',
            'league': 'NBA',
            'subject': 'Lebron James',
            'market': 'Points',
            'label': 'Under',
            'line': '8.5',
            'odds': 2.5,
            'impl_prb': 0.4
        },
        {
            'timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            'bookmaker': 'DraftKings',
            'league': 'NBA',
            'subject': 'Anthony Edwards',
            'market': 'Points',
            'label': 'Under',
            'line': '10.5',
            'odds': 1.15,
            'impl_prb': 0.87
        },
        {
            'timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            'bookmaker': 'DraftKings',
            'league': 'NBA',
            'subject': 'Anthony Davis',
            'market': 'Rebounds',
            'label': 'Under',
            'line': '10.5',
            'odds': 1.15,
            'impl_prb': 0.87
        },
        {
            'timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            'bookmaker': 'BoomFantasy',
            'league': 'MLB',
            'subject': 'Juan Soto',
            'market': 'Hits',
            'label': 'Over',
            'line': '1.5',
            'odds': 1.5,
            'impl_prb': 0.67
        }
    ]

    yield pd.DataFrame(betting_lines)


def _convert_to_set(betting_lines: Iterable) -> set[tuple]:
    return set([(k, v) for line in list(betting_lines) for k, v in line.items()])


def test_betting_lines(setup):
    session.betting_lines.storelines(setup)
    setup_list_dict = setup.to_dict(orient='records')
    setup_set_tuples = _convert_to_set(setup_list_dict)

    result = _convert_to_set(session.betting_lines.getlines())
    assert result == setup_set_tuples

    result = _convert_to_set(session.betting_lines.getlines(query={'bookmaker': 'PrizePicks'}))
    assert result == _convert_to_set([setup_list_dict[0]])

    result = _convert_to_set(session.betting_lines.getlines(query={'bookmaker': 'BoomFantasy'}))
    assert result == _convert_to_set([setup_list_dict[3]])

    result = _convert_to_set(session.betting_lines.getlines(query={'league': 'NBA'}))
    assert result == _convert_to_set([setup_list_dict[0], setup_list_dict[1], setup_list_dict[2]])

    result = _convert_to_set(session.betting_lines.getlines(query={'league': 'MLB'}))
    assert result == _convert_to_set([setup_list_dict[3]])

    result = _convert_to_set(session.betting_lines.getlines(query={'subject': 'Lebron James'}))
    assert result == _convert_to_set([setup_list_dict[0]])

    result = _convert_to_set(session.betting_lines.getlines(query={'subject': 'Anthony Edwards'}))
    assert result == _convert_to_set([setup_list_dict[1]])

    result = _convert_to_set(session.betting_lines.getlines(query={'market': 'Points'}))
    assert result == _convert_to_set([setup_list_dict[0], setup_list_dict[1]])

    result = _convert_to_set(session.betting_lines.getlines(query={'market': 'Hits'}))
    assert result == _convert_to_set([setup_list_dict[3]])

    result = _convert_to_set(session.betting_lines.getlines(query={'league': 'NBA', 'market': 'Points'}))
    assert result == _convert_to_set([setup_list_dict[0], setup_list_dict[1]])

    result = _convert_to_set(session.betting_lines.getlines(query={'league': 'NBA', 'market': 'Rebounds'}))
    assert result == _convert_to_set([setup_list_dict[2]])

    result = _convert_to_set(session.betting_lines.getlines(query={'bookmaker': 'DraftKings', 'market': 'Points'}))
    assert result == _convert_to_set([setup_list_dict[1]])