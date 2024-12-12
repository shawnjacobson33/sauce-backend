from typing import Optional
from collections import namedtuple

import redis

from app.ds.redis.structures import utils


class Subjects:
    def __init__(self, r: redis.Redis):
        self.__r = r

    def _get_subject_id(self, league: str, subj_attr: str, subject: str) -> Optional[str]:
        return self.__r.hget(f'subjects:std:{league}', key=f'{subj_attr}:{subject}')

    def get(self, league: str, subj_attr: str, subject: str, key: str = None) -> Optional[dict[str, str]]:
        if subj_id := self._get_subject_id(league, subj_attr, subject):
            return self.__r.hgetall(subj_id) if not key else self.__r.hget(subj_id, key=key)

        self._set_noid(league, subj_attr, subject)

    def get_unidentified(self) -> Optional[set[str]]:
        return self.__r.smembers('subjects:noid')

    def _set_noid(self, *args) -> None:
        self.__r.sadd('subjects:noid', 'subjects:{}:{}:{}'.format(*args))

    def _set_subject_id(self, subj: namedtuple) -> Optional[str]:
        s_id = utils.generate_id(self.__r, 'subjects')
        with self.__r.pipeline() as pipe:
            pipe.multi()
            subj_std_name = f'subjects:std:{subj.league}'
            for subj_name in [subj.name, subj.std_name]:
                # TODO: Think about when a subject's key needs to change...if they switch teams
                pipe.hset(subj_std_name, key=f'{subj.team}:{subj_name}', value=s_id)
                pipe.hset(subj_std_name, key=f'{subj.pos}:{subj_name}', value=s_id)

            pipe.execute()

        return s_id

    def rollback(self, subj: namedtuple):
        del_keys = 0
        subj_std_name = f'subjects:std:{subj.league}'
        for subj_name in [subj.name, subj.std_name]:
            del_keys += self.__r.hdel(subj_std_name, f'{subj.team}:{subj_name}', f'{subj.pos}:{subj_name}')

        if del_keys == 4:
            curr_id = self.__r.get('subjects:auto:id')
            self.__r.decrby('subjects:auto:id')
            self.__r.delete(f'subject:{int(curr_id)}')
            print(f"Subjects: Successfully deleted {subj.name} and {subj.std_name}!")
            return

        print(f"Subjects: Failed to delete {subj.name} and {subj.std_name}...they don't exist!")

    def store(self, subj: namedtuple) -> None:
        try:
            if s_id := self._set_subject_id(subj):
                if self.__r.hset(s_id, mapping={
                    **{key if key != 'std_name' else 'name': val for key, val in subj._asdict().items() if key != 'name'}
                }):
                    print(f"Subjects: Successfully stored '{subj.league}:{subj.std_name}'!")

        except AttributeError as e:
            print("Error:", e)
            self.__r.decrby('subjects:auto:id')