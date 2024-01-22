from abc import ABC
from abc import abstractmethod


class EDR_formatter(ABC):
    """
    This is the abstract class for implementing a formatter in the E-SOH EDR formatter
    Name of class should represent expected output format.
    """

    pass

    @abstractmethod
    def convert(self, datastore_reply):
        """
        Main method for converting protobuf object to given format.
        """
        pass
