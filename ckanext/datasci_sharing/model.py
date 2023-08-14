from typing import Optional

from sqlalchemy import (
    Table,
    Column,
    UnicodeText,
    ForeignKey,
    Boolean,
)

import ckan.model as model
from ckan.model import types, meta
from ckan.model.domain_object import DomainObject


package_sharing_policy_table = Table(
    'package_sharing_policy',
    meta.metadata,
    Column(
        'package_id',
        UnicodeText,
        ForeignKey(model.package_table.columns['id']),
        primary_key=True,
        default=types.make_uuid,
    ),
    Column('allowed', Boolean),
    Column('handle', UnicodeText, nullable=True),
)


class PackageSharingPolicy(DomainObject):
    def __init__(
        self,
        package_id: Optional[str],
        allowed: bool = False,
        handle: Optional[str] = None,
    ):
        self.package_id = package_id
        self.allowed = allowed
        self.handle = handle

    @classmethod
    def get_or_default(cls, package_id, for_update=False):
        query = model.Session.query(PackageSharingPolicy).filter_by(package_id=package_id)
        if for_update:
            query = query.with_for_update()
        return query.one_or_none() or PackageSharingPolicy(package_id=package_id)


meta.mapper(PackageSharingPolicy, package_sharing_policy_table)
