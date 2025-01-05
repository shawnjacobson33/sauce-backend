from datetime import datetime

import pandas as pd
import pytest

from app.cache import session


@pytest.fixture
def setup():
    session.client.delete('lines:info')
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


def test_betting_lines(setup):
    session.betting_lines.storelines(setup)
    setup = setup.to_dict(orient='records')

    result = list(session.betting_lines.getlines())
    assert result == setup

    result = list(session.betting_lines.getlines(query={'bookmaker': 'PrizePicks'}))
    assert result == [setup]

    result = list(session.betting_lines.getlines(query={'bookmaker': 'BoomFantasy'}))
    assert result == [setup[2]]

    result = list(session.betting_lines.getlines(query={'league': 'NBA'}))
    assert result == [setup[0], setup[1]]

    result = list(session.betting_lines.getlines(query={'league': 'MLB'}))
    assert result == [setup[3]]

    result = list(session.betting_lines.getlines(query={'subject': 'Lebron James'}))
    assert result == [setup[0]]

    result = list(session.betting_lines.getlines(query={'subject': 'Anthony Edwards'}))
    assert result == [setup[1]]

    result = list(session.betting_lines.getlines(query={'market': 'Points'}))
    assert result == [setup[0], setup[1]]

    result = list(session.betting_lines.getlines(query={'market': 'Hits'}))
    assert result == [setup[3]]

    result = list(session.betting_lines.getlines(query={'league': 'NBA', 'market': 'Points'}))
    assert result == [setup[0], setup[1]]

    result = list(session.betting_lines.getlines(query={'league': 'NBA', 'market': 'Rebounds'}))
    assert result == [setup[2]]

    result = list(session.betting_lines.getlines(query={'bookmaker': 'DraftKings', 'market': 'Points'}))
    assert result == [setup[1]]