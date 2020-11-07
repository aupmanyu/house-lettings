import uuid
from dataclasses import dataclass, field

from potential_tenants.app import general_constants


@dataclass
class User:
    uuid: uuid.UUID
    _uuid: uuid.UUID = field(default= uuid.uuid4(), init=False, repr=False)

    email: str
    _email: str = field(init=False, repr=False)

    max_rent: float
    _max_rent: float = field(init=False, repr=False)

    min_beds: int
    _min_beds: int = field(init=False, repr=False)

    move_date_low: str
    move_date_high: str
    move_date_range: (str, str)
    _move_date_range: (str, str) = field(init=False, repr=False)

    keywords: [str]
    _keywords: [general_constants.Keywords] = field(init=False, repr=False)

    desired_areas: [str]
    _desired_areas: [str] = field(init=False, repr=False)

    desired_cats: [str]
    _desired_cats: [general_constants.NhoodCategorisation] = field(init=False, repr=False)

    destinations: [dict]
    _destinations: [dict] = field(init=False, repr=False)

    @property
    def uuid(self) -> uuid.UUID:
        return self._uuid

    @property
    def email(self) -> str:
        return self._email

    @email.setter
    def email(self, email: str):
        self._email = email

    @property
    def max_rent(self) -> float:
        return self._max_rent

    @max_rent.setter
    def max_rent(self, max_rent: float):
        self._max_rent = max_rent

    @property
    def min_beds(self) -> int:
        return self._min_beds

    @min_beds.setter
    def min_beds(self, min_beds: int):
        self._min_beds = min_beds

    @property
    def move_date_low(self):
        return self._move_date_range[0]

    @property
    def move_date_high(self):
        return self._move_date_range[1]

    @property
    def move_date_range(self):
        return self._move_date_range

    @move_date_range.setter
    def move_date_range(self, date_range: tuple) -> (str, str):
        self._move_date_range = date_range

    @property
    def keywords(self):
        return [x.name for x in self._keywords]

    @keywords.setter
    def keywords(self, keywords: [str]) -> general_constants.Keywords:

