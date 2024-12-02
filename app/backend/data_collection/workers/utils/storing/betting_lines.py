import threading
from collections import defaultdict, deque, Counter
from datetime import datetime


class BettingLinesStore(dict):
    def __missing__(self, key):
        value = defaultdict(dict)
        self[key] = value
        return value

"""
{
    'date': '2024-11-24',
    'league': 'NBA',
    'game_info': 'BOS @ ATL',
    'market': 'Points',
    'subject': "Jayson Tatum",
    'bookmakers': {
        'PrizePicks': {
            'Over': [
                {
                    'tstamp': '2024-11-24 11:41:47.175330',
                    'line': 25.5,
                    'is_boosted': True
                },
                {
                    'tstamp': '2024-11-24 11:43:47.175330',
                    'line': 26.0,
                    'mult': 1.5,
                }
            ],
            'Under': [
                {
                    'tstamp': '2024-11-24 11:41:47.175330',
                    'line': 25.5,
                },
                {
                    'tstamp': '2024-11-24 11:43:47.175330',
                    'line': 26.0,
                    'mult': 1.5,
                }
            ]
        },
        'DraftKings': {
            'Over': [
                {
                    'tstamp': '2024-11-24 11:41:47.175330',
                    'line': 26.5,
                    'odds': 2.25,
                    'im_prb': 0.582,
                },
                {
                    'tstamp': '2024-11-24 11:43:47.175330',
                    'line': 26.0,
                    'odds': 2.18,
                    'im_prb': 0.565,
                }
            ]
            'Under': [
                {
                    'tstamp': '2024-11-24 11:41:47.175330',
                    'line': 26.5,
                    'odds': 2.25,
                    'im_prb': 0.582,
                },
                {
                    'tstamp': '2024-11-24 11:43:47.175330',
                    'line': 26.0,
                    'odds': 2.18,
                    'im_prb': 0.565,
                }
            ]
            
        },
    },
    'result': 30.0
}
"""
class BettingLines:
    _betting_lines = BettingLinesStore()
    _count = defaultdict(int)
    _lock = threading.Lock()

    @classmethod
    def get_lines(cls):
        return cls._betting_lines

    @classmethod
    def update(cls, betting_line: dict):
        with cls._lock:
            uniq_identifier = (betting_line['league'], betting_line['market'], betting_line['subject_id'])
            betting_line_record = {key: value for key, value in betting_line.items() if key in
                                   {'batch_id', 'line', 'mult', 'odds', 'im_prb', 'is_boosted'} and value
                                   }
            if betting_line_history := cls._betting_lines[uniq_identifier]:
                if bookmaker_history := betting_line_history['bookmakers'].setdefault(betting_line['bookmaker'], {}):
                    if label_specific_history := bookmaker_history.setdefault(betting_line['label'], deque()):
                        most_recent_bookmaker_history = label_specific_history[-1]
                        if {key: value for key, value in betting_line.items() if key != 'batch_id'} != most_recent_bookmaker_history:
                            label_specific_history.append(betting_line_record)
                        else:
                            # TODO: rethink this
                            most_recent_bookmaker_history['e_tstamp'] = betting_line['s_tstamp']
                    else:
                        label_specific_history.append(betting_line_record)
                else:
                    if 'dflt_odds' in betting_line:
                        bookmaker_history['dflt_odds'] = betting_line['dflt_odds']
                        bookmaker_history['dflt_im_prb'] = betting_line['dflt_im_prb']

                    bookmaker_history[betting_line['label']] = deque([betting_line_record])
            else:
                cls._betting_lines[uniq_identifier] = {
                    'birth': datetime.now().strftime('%Y-%m-%d'),
                    'sport': betting_line['sport'],
                    'league': betting_line['league'],
                    'game_time': betting_line['game_time'],
                    'game': betting_line['game'],
                    'market': betting_line['market'],
                    'player': betting_line['subject'],
                    'bookmakers': {
                        betting_line['bookmaker']: {
                            betting_line['label']: deque(
                                [betting_line_record]
                            )
                        }
                    }
                }
                if 'dflt_odds' in betting_line:
                    cls._betting_lines[uniq_identifier]['bookmakers'][betting_line['bookmaker']]['dflt_odds'] = betting_line[
                        'dflt_odds']
                    cls._betting_lines[uniq_identifier]['bookmakers'][betting_line['bookmaker']]['dflt_im_prb'] = \
                    betting_line[
                        'dflt_im_prb']

            cls._count[betting_line['bookmaker']] += 1

    @classmethod
    def counts(cls, bookmaker_name: str = None) -> int:
        total_count = 0
        if not bookmaker_name:
            for bookmaker_count in cls._count.values():
                total_count += bookmaker_count

        return cls._count[bookmaker_name] if bookmaker_name else total_count