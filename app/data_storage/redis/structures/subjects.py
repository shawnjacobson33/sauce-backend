from typing import Optional

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

    def _set_subject_id(self, subj_id: str, league: str, team: str, pos: str, subjects: list[str]) -> str:
        s_id = utils.get_short_id(self.__r, 'subjects')
        self.__r.hset(f'subjects:lookup:{league}', key=subj_id, value=s_id)
        subj_std_name = f'subjects:std:{league}'
        for subj in subjects:
            self.__r.hsetnx(subj_std_name, key=f'{team}:{subj}', value=s_id)
            self.__r.hsetnx(subj_std_name, key=f'{pos}:{subj}', value=s_id)

        return s_id

    def store(self, league: str, team: str, position: str, subject: str, std_subject: str) -> None:
        subj_id = f'subjects:{league}:{team}:{position}:{std_subject}'
        s_id = self._set_subject_id(subj_id, league, team, position, subjects=[subject, std_subject])
        self.__r.hset(s_id, mapping={
            'name': std_subject,
            'team': team
        })