import json
from typing import Optional, Union

import redis
from redis.client import Pipeline

from app.data_storage.stores.base import DynamicDataStore


class BoxScores(DynamicDataStore):
    def __init__(self, r: redis.Redis):
        """
        Initialize the BoxScores store.

        Args:
            r (redis.Redis): Redis client instance for interacting with the database.
        """
        super().__init__(r, 'box_scores')

    def getboxscore(self, league: str, subj_id: str, stat: str = None, expire: bool = False) -> \
            Optional[Union[str, dict]]:
        name = f'{self.name}:{league.lower()}'
        if box_score_json := self._r.hget(name, f'b{subj_id}'):
            box_score_dict = json.loads(box_score_json)
            if stat:
                return box_score_dict[stat]
            if expire:
                self._r.hdel(name, f'b{subj_id}')

            return box_score_dict

    @staticmethod
    def _evaluate_game_state(pipe: Pipeline, box_score: dict) -> None:
        if box_score['is_completed']:
            league, game_id = box_score['league'], box_score['game_id']
            pipe.sadd(f'games:completed:{league}', game_id)
            pipe.hdel(f'games:live:{league}', game_id)

    def storeboxscores(self, box_scores: list[dict]) -> None:
        try:
            with self._r.pipeline() as pipe:
                pipe.multi()
                for box_score in box_scores:
                    self._evaluate_game_state(pipe, box_score)
                    pipe.hset(f'{self.name}:{box_score["league"].lower()}', f'b{box_score["subj_id"]}',
                              json.dumps(box_score))
                pipe.execute()

        except KeyError as e:
            raise KeyError(f'Error storing box scores: {e}')
