from abc import ABC, abstractmethod


class EDR_formatter(ABC):
    """
    This is the abstract class for implementing a formatter in the E-SOH EDR formatter
    """
    pass

    @abstractmethod
    def convert():
        """
        Main method for converting protobuf object to given format.
        """
        pass
