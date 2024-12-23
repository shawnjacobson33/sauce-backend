from typing import Optional, Iterable

import redis

from app.data_storage.models import Team, Game
from app.data_storage.stores.base import DynamicDataStore


class BoxScores(DynamicDataStore):
    def __init__(self, r: redis.Redis):
        super().__init__(r, 'box_scores')

    def getboxscore(self, league: str, s_id: str = None, subj: str = None) -> Optional[Union[str, dict[str, Any]]]:
        if subj: s_id = self.std_mngr.get_eid(league, subj)
        if not s_id: raise ValueError(f'No id found for {subj} in {league}.')
        return self.get_entity('direct', s_id, league)

    def get_completed(self) -> Optional[set[str]]:
        return self._r.smembers('box_scores:index:completed')

    def _set_completed_lines(self, subj_id: str) -> None:
        completed_lines = self._r.smembers(f'lines:index:{subj_id}')
        for bl_id in completed_lines:
            self._r.sadd(f'lines:index:completed', bl_id)  # Don't need to index by league because box scores isn't indexed by league

    def store(self, s_id: str, box_score: dict[str, Any]) -> None:
        bx_id = f'box_scores:{s_id}'
        if box_score['is_completed']: # TODO: implement logic in box score retrievers to set is_completed field
            self._set_completed_lines(s_id)
            self._r.sadd('box_scores:index:completed', bx_id)
            # TODO: CHoose expiration time or implement another way of removing the data

        if self._r.sismember('subjects:active', s_id):
            self._r.hset(bx_id, mapping=box_score)
