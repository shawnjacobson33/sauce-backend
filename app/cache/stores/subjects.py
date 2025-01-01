import json
from collections import defaultdict
from typing import Optional, Iterable

import redis

from app.cache.models import Subject
from app.cache.stores.base import DataStore


class Subjects(DataStore):
    def __init__(self, r: redis.Redis):
        super().__init__(r, 'subjects')

    @staticmethod
    def _get_key(subject: Subject) -> str:
        condensed_name = subject.name.replace(' ', '')
        if subj_attr := position if (position := subject.position) else team if (team := subject.team) else None:
            return f'{subj_attr}:{condensed_name}'

        return condensed_name

    def getid(self, subject: Subject) -> Optional[str]:
        return self._r.hget(f'{self.lookup_name}:{subject.domain.lower()}', self._get_key(subject))

    def _scan_subj_ids(self, league: str) -> Iterable:
        counter = defaultdict(int)
        for _, subj_id in self._r.hscan_iter(f'{self.lookup_name}:{league.lower()}'):
            if not counter[subj_id]:
                counter[subj_id] += 1
                yield subj_id

    def getids(self, league: str) -> Iterable:
        yield from self._scan_subj_ids(league)

    def getsubject(self, league: str, subject: Subject, report: bool = False) -> Optional[dict]:
        if subj_id := self.getid(subject):
            if subj := self._r.hget(f'{self.info_name}:{league.lower()}', subj_id):
                return json.loads(subj)

    def getsubjs(self, league: str) -> Iterable:
        if subj_ids := self.getids(league):
            for subj_id in subj_ids:
                if subj_json := self._r.hget(f'{self.info_name}:{league.lower()}', subj_id):
                    yield json.loads(subj_json)


    def setsubjactive(self, league: str, subj_id: str) -> None:
        self._r.sadd(f'{self.name}:active:{league.lower()}', subj_id)

    @staticmethod
    def _get_keys(subject: Subject) -> tuple[str, str]:
        if (position := subject.position) and (team := subject.team):
            condensed_name = subject.name.replace(' ', '')
            return f'{position}:{condensed_name}', f'{team}:{condensed_name}'

    def _store_in_lookup(self, league: str, subj: Subject) -> Optional[str]:
        try:
            inserts = 0
            subj_id = self.id_mngr.generate()
            for subj_key in self._get_keys(subj):
                inserts += self._r.hsetnx(f'{self.lookup_name}:{league.lower()}', subj_key, subj_id)

            return subj_id if inserts else self.id_mngr.decr()

        except IndexError as e:
            self.id_mngr.decr()
            print(f"Error extracting subj keys: {subj} -> {e}")

    def storesubjects(self, league: str, subjects: list[Subject]) -> None:
        try:
            with self._r.pipeline() as pipe:
                pipe.multi()
                for subj in subjects:
                    if subj_id := self._store_in_lookup(league, subj):
                        pipe.hset(f'{self.info_name}:{league.lower()}', subj_id, json.dumps({
                        'name': subj.std_name.split(':')[-1],
                        'team': subj.team,
                    }))
                pipe.execute()

        except AttributeError as e:
            self._handle_error(e)
