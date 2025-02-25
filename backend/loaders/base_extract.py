from abc import ABC, abstractmethod
from datetime import datetime
from typing import Union

class BaseExtract(ABC):

    def __init__(
            self,
            start_date: Union[str, int] = None,
            end_date: Union[str, int] = None
    ):
        """
        Base class for data extractor.

        Args:
            start_date (Union[str, int], optional): Start date. Defaults to None.
            end_date (Union[str, int], optional): End date. Defaults to None.
        """
        self.dt = self.parse_date(start_date)
        self.dt_end = self.parse_date(end_date)

    @abstractmethod
    def extract(self):
        # The extract function is an abstract method and it must be added to each child class.
        ...

    @staticmethod
    def parse_date(date_string: Union[str, None]) -> Union[datetime, None]:
        """
        Parse date string to datetime object.

        Args:
            date_string (Union[str, None]): Date string.

        Returns:
            Union[datetime, None]: Datetime object.

        Raises:
            DateFormatException: If date format is not understood.
        """
        if date_string is None:
            return None
        formatting_error = (
            f"Format for {date_string} not understood. "
            f"Accepted format are '%Y%m%d' or '%Y%m%d%H' "
            f"(e.g. 20210328 or 2021032815)."
        )
        try:
            int(date_string)
        except ValueError:
            raise DateFormatException(formatting_error)

        date_string = str(date_string)
        if len(date_string) == 8:
            date_format = "%Y%m%d"
        elif len(date_string) == 10:
            date_format = "%Y%m%d%H"
        else:
            raise DateFormatException(formatting_error)

        return datetime.strptime(date_string, date_format)

class DateFormatException(Exception):
    """
    Raised when date format is not understood
    """