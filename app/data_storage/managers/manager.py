import redis


class Manager:
    """
    A class to manage operations related to a Redis instance and track its activity.

    Attributes:
        _r (redis.Redis): The Redis connection instance used to interact with the database.
        _name (str): The name associated with this manager, used to generate keys or manage the domain.
        performed_insert (bool): A flag indicating whether an insert operation has been performed.
        updates (int): A counter to track the number of updates performed.
    """
    def __init__(self, r: redis.Redis, name: str):
        """
        Initializes the Manager instance.

        Args:
            r (redis.Redis): The Redis instance to interact with.
            name (str): The initial name associated with this Manager.
        """
        self._r = r
        self._name = name

        self.performed_insert = False
        self.updates = 0

    @property
    def name(self) -> str:
        return self._name

    @name.setter
    def name(self, domain: str = None) -> None:
        """
        Modifies the current `name` by appending or replacing the domain.

        If the `name` contains two colons (`:`), the domain is appended directly.
        If the `name` has more than two segments (based on colons), the last segment is replaced with the domain.

        Args:
            domain (str): The domain to append or replace in the `name`.

        Returns:
            str: The modified `name` with the domain included.
        """
        if domain:
            if self._name.count(':') == 2:
                self._name = f'{self._name}:{domain.lower()}'
            else:
                partial_name = ':'.join(self._name.split(':')[:-1])
                self._name = f'{partial_name}:{domain.lower()}'
