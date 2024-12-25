from typing import Optional, Iterable

import redis

from app.data_storage.models import Subject
from app.data_storage.stores.base import StaticDataStore


class Subjects(StaticDataStore):
    """
    A data store class for managing Subject entities in a Redis database.
    """
    def __init__(self, r: redis.Redis):
        """
        Initializes the Subjects data store.

        Args:
            r (redis.Redis): A Redis client instance.
        """
        super().__init__(r, 'subjects')

    @staticmethod
    def _get_key(subject: Subject) -> str:
        """
        Generates a unique key for a given subject based on its attributes.

        Args:
            subject (Subject): The Subject instance to generate the key for.

        Returns:
            str: The generated key, which prioritizes `position` > `team` > `name`.
        """
        condensed_name = subject.name.replace(' ', '')
        if subj_attr := position if (position := subject.position) else team if (team := subject.team) else None:
            return f'{subj_attr}:{condensed_name}'

        return condensed_name

    def getid(self, subject: Subject) -> Optional[str]:
        """
        Retrieves the unique ID of a subject.

        Args:
            subject (Subject): The Subject instance to retrieve the ID for.

        Returns:
            Optional[str]: The unique ID of the subject if found, otherwise `None`.
        """
        return self.std_mngr.get_eid(subject.domain, Subjects._get_key(subject))

    def getids(self, league: str = None) -> Iterable:
        """
        Retrieves all unique IDs of subjects for a given league.

        Args:
           league (str, optional): The league name to filter subjects by.

        Yields:
           Iterable: An iterable of unique subject IDs.
        """
        yield from self.std_mngr.get_eids(league)

    def getsubj(self, subject: Subject, report: bool = False) -> Optional[str]:
        """
        Retrieves the detailed representation of a subject.

        Args:
            subject (Subject): The Subject instance to retrieve.
            report (bool, optional): Whether to include reporting information.

        Returns:
            Optional[str]: The subject details if found, otherwise `None`.
        """
        return self.get_entity('secondary', subject.domain, Subjects._get_key(subject), report=report)

    def getsubjs(self, league: str) -> Iterable:
        """
        Retrieves detailed representations of all subjects in a league.

        Args:
            league (str): The league name to filter subjects by.

        Yields:
            Iterable: An iterable of detailed subject representations.
        """
        yield from self.get_entities('secondary', league)

    @staticmethod
    def _get_keys(subject: Subject) -> tuple[str, str]:
        """
        Generates multiple keys for a subject based on its attributes.

        Args:
            subject (Subject): The Subject instance to generate keys for.

        Returns:
            Iterable: An iterable of generated keys.
        """
        if (position := subject.position) and (team := subject.team):
            condensed_name = subject.name.replace(' ', '')
            return f'{position}:{condensed_name}', f'{team}:{condensed_name}'

    def setsubjactive(self, s_id: str) -> None:
        self._r.sadd(f'{self.name}:active', s_id)

    def store(self, league: str, subjects: list[Subject]) -> None:
        """
        Stores a batch of subjects in the database for a given league.

        Args:
            league (str): The league name associated with the subjects.
            subjects (list[Subject]): A list of Subject instances to store.

        Raises:
            AttributeError: If there is an issue with the subject attributes.
        """
        try:
            with self._r.pipeline() as pipe:
                pipe.multi()
                for s_id, subj in self.std_mngr.store_eids(league, subjects, keys=Subjects._get_keys):
                    pipe.hset(s_id, mapping={
                        'name': subj.std_name.split(':')[-1],
                        'team': subj.team,
                    })

                pipe.execute()

        except AttributeError as e:
            self._handle_error(e)
