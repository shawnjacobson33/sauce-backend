from collections import defaultdict
import threading
from collections import deque
from datetime import datetime

from backend.app.data_collection.workers.utils.storing.reporting import LineWorkerStats


class BettingLines:
    _lines = defaultdict(dict)
    _count = defaultdict(int)
    _lock = threading.Lock()

    @classmethod
    def get_lines(cls):
        return cls._lines

    @classmethod
    def update(cls, betting_line: dict):
        with (cls._lock):
            line_worker_stats = defaultdict(int)
            line_worker_stats['total_lines'] += 1

            unique_identifier = (betting_line['league'], betting_line['market'], betting_line['subject_id'])
            betting_line_record = cls._create_betting_line_record(betting_line)

            history_exists = unique_identifier in cls._lines
            if history_exists:
                bookmaker_history = cls._lines[unique_identifier]['bookmakers']
                label_history = bookmaker_history.setdefault(betting_line['bookmaker'], {}).setdefault(
                    (betting_line['label'], betting_line['line']), deque([betting_line_record]))

                most_recent = label_history[-1] if label_history else None
                is_duplicate = betting_line_record == most_recent
                if not is_duplicate: # This will help when running, for instance, Sleeper with PropProfessor
                    is_different = False
                    for key, val in betting_line.items():
                        if (key != 'batch_ids') and (val != most_recent.get(key)):
                            is_different = True
                            break

                    if is_different:
                        label_history.append(betting_line_record)
                        line_worker_stats['line_changes'] += 1
                    else:
                        most_recent['batch_ids'].extend(betting_line_record['batch_ids'])

            else:
                cls._initialize_betting_line(unique_identifier, betting_line, betting_line_record)
                line_worker_stats['new_lines'] += 1

            batch_id = betting_line['batch_ids'][-1]
            LineWorkerStats.update_line_counts(betting_line['bookmaker'], batch_id=batch_id, **line_worker_stats)
            cls._count[betting_line['bookmaker']] += 1

    @classmethod
    def _create_betting_line_record(cls, betting_line):
        fields = {'batch_ids', 'mult', 'odds', 'im_prb', 'is_boosted'}
        return {key: value for key, value in betting_line.items() if key in fields and value}

    @classmethod
    def _initialize_betting_line(cls, unique_identifier, betting_line, betting_line_record):
        cls._lines[unique_identifier] = {
            'discovered': datetime.now().strftime('%Y-%m-%d'),
            'sport': betting_line['sport'],
            'league': betting_line['league'],
            'game_time': betting_line['game_time'],
            'game': betting_line['game'],
            'market': betting_line['market'],
            'player': betting_line['subject'],
            'bookmakers': {
                betting_line['bookmaker']: {
                    (betting_line['label'], betting_line['line']): deque([betting_line_record])
                }
            },
        }

        if 'stats' in betting_line:
            betting_line['curr_stat'] = cls._extract_curr_stat(betting_line['market'], betting_line['stats'])

        if 'dflt_odds' in betting_line:
            cls._lines[unique_identifier]['bookmakers'][betting_line['bookmaker']]['dflt_odds'] = betting_line[
                'dflt_odds']
            cls._lines[unique_identifier]['bookmakers'][betting_line['bookmaker']]['dflt_im_prb'] = \
            betting_line['dflt_im_prb']

    @classmethod
    def _extract_curr_stat(cls, market: str, stats: dict):

    @classmethod
    def counts(cls, bookmaker_name: str = None) -> int:
        total_count = 0
        if not bookmaker_name:
            for bookmaker_count in cls._count.values():
                total_count += bookmaker_count

        return cls._count[bookmaker_name] if bookmaker_name else total_count


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