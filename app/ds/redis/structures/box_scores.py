from typing import Optional, Any, Union

import redis


class BoxScores:
    def __init__(self, r: redis.Redis):
        self.__r = r

    def get(self, subj_id: str, stat: str = None) -> Optional[Union[str, dict[str, Any]]]:
        bx_id = f'box_scores:{subj_id}'
        if box_score := self.__r.hgetall(bx_id) if not stat else self.__r.hget(bx_id, stat):
            return box_score

    def get_completed(self) -> Optional[set[str]]:
        return self.__r.smembers('box_scores:index:completed')

    def _set_completed_lines(self, subj_id: str) -> None:
        completed_lines = self.__r.smembers(f'lines:index:{subj_id}')
        for bl_id in completed_lines:
            self.__r.sadd(f'lines:index:completed', bl_id)  # Don't need to index by league because box scores isn't indexed by league

    def store(self, s_id: str, box_score: dict[str, Any]) -> None:
        bx_id = f'box_scores:{s_id}'
        if box_score['is_completed']: # TODO: implement logic in box score retrievers to set is_completed field
            self._set_completed_lines(s_id)
            self.__r.sadd('box_scores:index:completed', bx_id)
            # TODO: CHoose expiration time or implement another way of removing the data

        if self.__r.sismember('subjects:active', s_id):
            self.__r.hset(bx_id, mapping=box_score)
