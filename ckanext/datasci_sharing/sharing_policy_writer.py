import copy
import logging
from typing import Optional, List

from .policy_elements import PolicyDocumentObject, PolicyDocumentSizeLimitExceeded, wrap_policy_document


logger = logging.getLogger(__name__)

def _arn_to_name(arn: str):
    return arn.split(":", maxsplit=5)[-1]


def _policy_target_bucket_name(document: PolicyDocumentObject):
    arn = document.statement.first_where(sid=PolicyDocumentObject.LISTING_SID).resource.single()
    return _arn_to_name(arn)


class SharingPolicyWriter:
    """Helper class to update sharing permissions."""
    # TODO encapsulate this class' functionality within the domain objects
    def __init__(self, *, document: PolicyDocumentObject):
        self._document = document
        self._bucket_name = _policy_target_bucket_name(self._document)
        self._closed = False

    def update_prefix_policy(self, prefix: str, *, allow: bool) -> 'SharingPolicyWriter':
        self._update_listing_statement(prefix, allow)
        self._update_actions_statement(prefix, allow)
        self._document.raise_for_size()

    def _update_listing_statement(self, prefix: str, allow: bool):
        listing = self._document.statement.first_where(sid=PolicyDocumentObject.LISTING_SID)
        prefixes_set = set(listing.condition.string_equals['s3:prefix'])
        if allow:
            prefixes_chain = self._prefixes_chain_from_prefix(prefix)
            prefixes_set.update(prefixes_chain)
        else:
            # only remove the prefix for the package itself because prefixes
            # before the package may be common with other shared packages
            prefixes_set.discard(prefix)
        listing.condition.string_equals['s3:prefix'].unwrap()[:] = prefixes_set

    def _update_actions_statement(self, prefix: str, allow: bool):
        actions = self._document.statement.first_where(sid=PolicyDocumentObject.ACTIONS_SID)
        resources_set = set(actions.resource)
        resource_arn = f'arn:aws:s3:::{self._bucket_name}/{prefix}/*'
        if allow:
            resources_set.add(resource_arn)
        else:
            resources_set.discard(resource_arn)
        actions.resource.unwrap()[:] = list(resources_set)

    def _prefixes_chain_from_prefix(self, prefix: str) -> List[str]:
        """
        Returns slices of the prefix.

        For example, the prefix `'/a/b/c'` will be converted into the list `['/a', '/a/b', '/a/b/c']`.
        """
        components = prefix.split('/')
        return [
            '/'.join(components[:index+1]) for index in range(len(components))
        ]
