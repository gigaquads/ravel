from typing import Type

from appyratus.utils.time_utils import TimeUtils

from ravel import Resource, Id, relationship, fields
from ravel.query.predicate import Predicate


class Session(Resource):
    is_active = fields.Bool(nullable=False, default=lambda: True)
    logged_out_at = fields.DateTime(nullable=True, default=lambda: None)

    @classmethod
    def __abstract__(cls):
        return True

    def logout(self):
        if self.is_active:
            self.update(logged_out_at=TimeUtils.utc_now(), is_active=False)
