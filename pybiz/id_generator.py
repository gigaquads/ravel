import uuid

from abc import ABCMeta, abstractmethod


class IdGenerator(object, metaclass=ABCMeta):

    @abstractmethod
    def next_id(self):
        """
        Generate and return a new ID.
        """

    @abstractmethod
    def next_public_id(self):
        """
        Generate and return a new ID.
        """


class UuidGenerator(IdGenerator):

    def next_id(self):
        return uuid.uuid4().hex

    def next_public_id(self):
        return uuid.uuid4().hex
