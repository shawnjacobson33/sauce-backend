import json
from typing import Iterable, Callable

import redis

from app.cache.stores.base import DataStore


LINE_ID_ORDERED_FIELDS = ['league', 'game_id', 'subj_id', 'market', 'label', 'line']


class BettingLines(DataStore):
    def __init__(self, r: redis.Redis):
        super().__init__(r, 'lines')

    def getids(self, bookmaker: str) -> Iterable:
        yield from self._r.hscan_iter(f'{self.info_name}:{bookmaker}', no_values=True)

    @staticmethod
    def _add_id_info_to_dict(bookmaker: str, line_id: str, line: dict) -> None:
        line['bookmaker'] = bookmaker
        for line_id_field, line_id_attr in zip(LINE_ID_ORDERED_FIELDS, line_id.split(':')):
            line[line_id_field] = line_id_attr

    def getlines(self, bookmaker: str, match: str = None) -> Iterable:
        pattern = f'*{match}*' if match else '*'
        for line_id, line_json in self._r.hscan_iter(f'{self.info_name}:{bookmaker}', match=pattern):
            line_dict = json.loads(line_json)
            self._add_id_info_to_dict(bookmaker, line_id.decode('utf-8'), line_dict)
            yield line_dict

    # def labellines(self, bookmaker: str, box_score: dict) -> Iterable:
    #     try:
    #         for line_id in self.getids(bookmaker):
    #             if line_json := self._r.hget(f'{self.info_name}:{bookmaker}', line_id):
    #                 line_dict = json.loads(line_json)
    #                 line_dict['result'] = box_score[line_dict['market']]
    #
    #                 self._r.hdel(f'{self.info_name}:{bookmaker}', self._get_key(line_dict))
    #                 yield line_dict
    #
    #     except KeyError as e:
    #         self._log_error(e)

    @staticmethod
    def _get_key(line: dict) -> str:
        return f'{line['league']}:{line['game_id']}:{line['subj_id']}:{line['market']}:{line['label']}:{line['line']}'

    def storelines(self, lines: list[dict] = None, func: Callable = None) -> None:
        try:
            with self._r.pipeline() as pipe:
                pipe.multi()
                data = lines if lines else func()
                for line in data:
                    novel_line_info = json.dumps({k: v for k, v in line.items() if k in ['timestamp', 'dflt_odds',
                                                                                         'odds', 'multiplier']})
                    pipe.hset(f'{self.info_name}:{line['bookmaker']}', self._get_key(line),
                              novel_line_info)
                pipe.execute()

        except KeyError as e:
            self._log_error(e)
