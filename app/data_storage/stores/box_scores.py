from typing import Optional, Union

import redis

from app.data_storage.stores.base import DynamicDataStore


class BoxScores(DynamicDataStore):
    """
    BoxScores provides methods to store and retrieve box scores for specific leagues and subjects.
    Box scores are stored in a Redis database and can be accessed using subject IDs or keys.
    """
    def __init__(self, r: redis.Redis):
        """
        Initialize the BoxScores store.

        Args:
            r (redis.Redis): Redis client instance for interacting with the database.
        """
        super().__init__(r, 'box_scores')

    def getboxscore(self, s_id: str, stat: str = None) -> \
            Optional[Union[str, dict]]:
        """
        Retrieve box score data for a specific subject or stat in a given league.

        Args:
            s_id (str, optional): The subject ID for the desired box score. Defaults to None.
            stat (str, optional): A specific stat to retrieve. If None, all stats are returned. Defaults to None.

        Returns:
            Optional[Union[str, dict]]: The specific stat value as a string if `stat` is provided,
                                        or a dictionary of all stats if `stat` is None.

        Raises:
            ValueError: If neither `s_id` nor `subj` is provided, or if the subject ID cannot be resolved.
        """
        if s_id:
            return self._r.hgetall(f'b{s_id}') if not stat else self._r.hget(f'b{s_id}', stat)

        raise ValueError('No id or subj provided to retrieve box score.')

    def remove(self, s_id: str) -> None:
        # will be called by the betting lines class when notified that a game is over and the betting line labelling
        # process is complete
        self._r.delete(f'b{s_id}')

    def store(self, box_scores: list[dict]) -> None:
        """
        Store a list of box scores for a specific league.

        Args:
            box_scores (list[dict]): A list of dictionaries representing box scores. Each dictionary
                                     must include a 'subj' key for the subject.

        Raises:
            KeyError: If a required key is missing in the box score data.
        """
        try:
            with self._r.pipeline() as pipe:
                pipe.multi()
                for box_score in box_scores:
                    if box_score['is_completed']:
                        pipe.sadd('games:completed', box_score['g_id'])
                        continue
                    pipe.hset(f'b{box_score["s_id"]}', mapping=box_score)
                pipe.execute()

        except KeyError as e:
            raise KeyError(f'Error storing box scores: {e}')
