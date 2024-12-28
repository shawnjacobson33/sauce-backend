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

    def getboxscore(self, league: str, subj_id: str, stat: str = None) -> \
            Optional[Union[str, dict]]:
        if box_score_json := self._r.hget(f'{self.name}:info:{league.lower()}', f'b{subj_id}'):
            box_score_dict = json.loads(box_score_json)
            if stat: return box_score_dict[stat]
            return box_score_dict

    @staticmethod
    def _evaluate_game_state(league: str, pipe: Pipeline, box_score: dict) -> None:
        if box_score['is_completed']:
            if game_json := pipe.hget(f'games:info:{league.lower()}', box_score['game_id']):
                game_dict = json.loads(game_json)
                game_dict['is_completed'] = True
                pipe.hset(f'games:info:{league.lower()}', box_score['game_id'], json.dumps(game_dict))

    def storeboxscores(self, league: str, box_scores: list[dict]) -> None:
        try:
            with self._r.pipeline() as pipe:
                pipe.multi()
                for box_score in box_scores:
                    self._evaluate_game_state(league, pipe, box_score)
                    pipe.hset(f'{self.name}:info:{league.lower()}', f'b{box_score["subj_id"]}',
                              json.dumps(box_score))
                pipe.execute()

        except KeyError as e:
            raise KeyError(f'Error storing box scores: {e}')
