import json
from typing import Iterable

import redis
import pandas as pd

from app.cache.stores.base import DataStore


LINE_ID_ORDERED_FIELDS = ['bookmaker', 'league', 'subject', 'market', 'label', 'line']


class BettingLines(DataStore):
    def __init__(self, r: redis.Redis):
        super().__init__(r, 'lines')

    def getids(self, bookmaker: str) -> Iterable:
        yield from self._r.hscan_iter(f'{self.info_name}:{bookmaker}', no_values=True)

    @staticmethod
    def _add_id_info_to_dict(line_id: str, line: dict) -> None:
        for line_id_field, line_id_attr in zip(LINE_ID_ORDERED_FIELDS, line_id.split(':')):
            line[line_id_field] = line_id_attr

    @staticmethod
    def _get_query(query: dict[str, str]) -> str:
        pattern = '*'
        for q in query.values():
            pattern += f'{q}*'

        return pattern

    def getlines(self, query: dict[str, str] = None):
        pattern = self._get_query(query) if query else '*'  # Todo: limitation...can only query for one value per field
        for line_id, line_json in self._r.hscan_iter(self.info_name, match=pattern):
            line_dict = json.loads(line_json)
            self._add_id_info_to_dict(line_id.decode('utf-8'), line_dict)
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
        return (
            f'{line['bookmaker']}:{line['league']}:{line['subject']}:{line['market']}:{line['label']}:{line['line']}'
        )

    def storelines(self, lines: pd.DataFrame) -> None:
        try:
            with self._r.pipeline() as pipe:
                pipe.multi()
                for line in lines.to_dict(orient='records'):
                    novel_line_info = json.dumps({k: v for k, v in line.items() if k in ['timestamp', 'dflt_odds',
                                                                                         'odds', 'multiplier', 'ev',
                                                                                         'impl_prb', 'tw_prb']})
                    pipe.hset(self.info_name, self._get_key(line), novel_line_info)

                pipe.execute()

        except KeyError as e:
            self._log_error(e)

        except Exception as e:
            raise e
