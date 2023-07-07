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
    def __init__(self, *, bucket_name: Optional[str] = None, document: Optional[dict] = None):
        self._document = (
            wrap_policy_document(copy.deepcopy(document))
            if document is not None
            else PolicyDocumentObject.new(bucket_name)
        )
        self._bucket_name = bucket_name or _policy_target_bucket_name(self._document)
        self._closed = False

    def update_prefix_policy(self, prefix: str, *, allow: bool) -> 'SharingPolicyWriter':
        self._check_not_closed()

        self._update_listing_statement(prefix, allow)
        self._update_actions_statement(prefix, allow)

        try:
            self._document.raise_for_size()
        except PolicyDocumentSizeLimitExceeded:
            self.close()
            raise

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

    def close(self) -> PolicyDocumentObject:
        self._check_not_closed()
        self._closed = True
        return self._document

    def _check_not_closed(self):
        if self._closed:
            raise RuntimeError("cannot operate with a closed writer")
