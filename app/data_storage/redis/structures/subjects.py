from typing import Optional
from collections import namedtuple

import redis

from app.data_storage.redis.structures import utils


class Subjects:
    def __init__(self, r: redis.Redis):
        self.__r = r

    def _get_subject_id(self, league: str, subj_attr: str, subject: str) -> Optional[str]:
        return self.__r.hget(f'subjects:std:{league}', key=f'{subj_attr}:{subject}')

    def get(self, league: str, subj_attr: str, subject: str, key: str = None) -> Optional[dict[str, str]]:
        if subj_id := self._get_subject_id(league, subj_attr, subject):
            return self.__r.hgetall(subj_id) if not key else self.__r.hget(subj_id, key=key)

        self._set_unidentified(league, subj_attr, subject)

    def get_unidentified(self) -> Optional[set[str]]:
        return self.__r.smembers('subjects:noid')

    def _set_unidentified(self, *args) -> None:
        self.__r.sadd('subjects:noid', 'subjects:{}:{}:{}'.format(*args))

    def _set_subject_id(self, subj: namedtuple) -> str:
        try:
            if not self.__r.hget(f'teams:std:{subj.league}', subj.name):
                s_id = utils.get_auto_id(self.__r, 'subjects')
                with self.__r.pipeline() as pipe:
                    pipe.multi()
                    subj_std_name = f'subjects:std:{subj.league}'
                    for subj in [subj.name, subj.std_name]:
                        pipe.hset(subj_std_name, key=f'{subj.team}:{subj}', value=s_id)
                        pipe.hset(subj_std_name, key=f'{subj.pos}:{subj}', value=s_id)

                    pipe.execute()

                return s_id

            print(f'Subject: {subj.name} already stored!')

        except AttributeError as e:
            print(e)

    def store(self, subj: namedtuple) -> None:
        if s_id := self._set_subject_id(subj):
            self.__r.hset(s_id, mapping={
                **{key if key != 'std_name' else 'name': val for key, val in subj.__dict__.items() if key != 'name'}
            })