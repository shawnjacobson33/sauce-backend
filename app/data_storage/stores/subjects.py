from typing import Optional, Iterable

import redis

from app.data_storage.models import Subject
from app.data_storage.stores.base import L2StaticDataStore


class Subjects(L2StaticDataStore):
    def __init__(self, r: redis.Redis):
        super().__init__(r, 'subjects')

    @staticmethod
    def _get_key(subject: Subject) -> str:
        if subj_attr := position if (position := subject.position) else team if (team := subject.team) else None:
            return f'{subj_attr}:{subject.name}'

        return subject.name

    def getid(self, subject: Subject) -> Optional[str]:
        return self.geteid(subject.domain, Subjects._get_key(subject))

    def getids(self, league: str = None) -> Iterable:
        yield from self.geteids(league)

    def getsubj(self, subject: Subject, report: bool = False) -> Optional[str]:
        return self.getentity(subject.domain, Subjects._get_key(subject), report=report)

    def getsubjs(self, league: str) -> Iterable:
        yield from self.getentities(league)

    # TODO: Yet to Implement
    def store(self, league: str, subjects: list[Subject]) -> None:
        try:
            with self.__r.pipeline() as pipe:
                pipe.multi()
                for s_id, subj in self._get_eids(league, subjects):
                    pipe.hset(s_id, mapping={
                        'name': subj.std_name,
                        'full': subj.full_name
                    })

                pipe.execute()

        except AttributeError as e:
            self._handle_error(e)