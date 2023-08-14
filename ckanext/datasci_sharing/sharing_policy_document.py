import typing as t
from collections import abc
import json


def _wrap_policy_document(value):
    if isinstance(value, dict):
        if value.get('Sid') is not None:
            return _PolicyDocumentStatement(value)
        return _PolicyDocumentDict(value)
    if isinstance(value, list):
        return _PolicyDocumentArray(value)
    return value


_T = t.TypeVar('_T')
_V = t.TypeVar('_V')


class PolicyDocumentSizeLimitExceeded(Exception):
    _POLICY_DOCUMENT_SIZE_LIMIT = 6 * 1024 # 6 KB

    def __init__(self):
        super().__init__(f"policy document size limit exceeded")

    @classmethod
    def check(cls, document: 'SharingPolicyDocument'):
        if document.size() > cls._POLICY_DOCUMENT_SIZE_LIMIT:
            raise PolicyDocumentSizeLimitExceeded()


class _PolicyDocumentBase(abc.Sized, t.Generic[_T]):
    __slots__ = ('_data',)

    def __init__(self, data: _T):
        self._data = data

    def unwrap(self) -> _T:
        return self._data

    def __len__(self):
        return len(self._data)

    def __repr__(self):
        return f"<{type(self).__name__} {self._data}>"


class _PolicyDocumentDict(abc.Mapping, _PolicyDocumentBase[dict]):
    def __getitem__(self, key):
        return _wrap_policy_document(self._data[key])

    def __iter__(self):
        return iter(self._data)


class _PolicyDocumentArray(abc.Sequence, _PolicyDocumentBase[t.List[_V]], t.Generic[_T, _V]):
    def first_where(self, **kwargs) -> _T:
        if len(kwargs) != 1:
            raise ValueError('cannot filter by more than one property')
        [(key, value)] = kwargs.items()
        return next(
            element for element in self
            if element[key] == value
        )

    def single(self) -> _T:
        if len(self) != 1:
            raise ValueError("expected a single element")
        return self[0]

    def __getitem__(self, index) -> _T:
        return _wrap_policy_document(self._data[index])

    def __iter__(self):
        for item in self._data:
            yield _wrap_policy_document(item)


_BUCKETS_LISTING_SID = 'A'
_PACKAGES_LISTING_SID = 'B'
_PACKAGES_ACTIONS_SID = 'C'


class SharingPolicyDocument(_PolicyDocumentDict):
    __slots__ = ('_data', 'handle')

    def __init__(self, data, handle):
        super().__init__(data)
        self.handle = handle

    @staticmethod
    def new(bucket_name: str, handle: str) -> 'SharingPolicyDocument':
        return _new_policy_document_object(bucket_name, handle)

    def bucket_arn(self) -> str:
        return self._statement(_PACKAGES_LISTING_SID).resources().single()

    def size(self) -> int:
        """Returns the count of characters within the document excluding whitespace."""
        return len(json.dumps(self._data, separators=(',', ':')))

    def _raise_for_size(self):
        """Ensures that the document size limit has not been exceeded."""
        PolicyDocumentSizeLimitExceeded.check(self)

    def as_json(self):
        return json.dumps(self._data)

    def allow_buckets_listing(self):
        """Add a statement to allow listing buckets within AWS console."""
        self._statements().unwrap().insert(0, _BUCKETS_LISTING_STATEMENT)

    def update_prefix(self, prefix: str, allow: bool):
        listing = self._statement(_PACKAGES_LISTING_SID)
        prefixes_set = set(listing.prefixes_condition())
        prefixes_chain = self._prefixes_chain_from_prefix(prefix)
        if allow:
            prefixes_set.update(prefixes_chain)
        else:
            # only remove the prefix (and the "catch all" prefix for sub-prefixes)
            # for the package itself because prefixes before the package may be
            # common with other shared packages
            prefixes_set.difference_update(prefixes_chain[-2:])
        listing.prefixes_condition().unwrap()[:] = prefixes_set

        actions = self._statement(_PACKAGES_ACTIONS_SID)
        resources_set = set(actions.resources())
        if allow:
            resources_set.add(self._resource_arn_from_prefix(prefix))
            resources_set.discard(self._null_resource())
        else:
            resources_set.discard(self._resource_arn_from_prefix(prefix))
            if not resources_set:
                resources_set.add(self._null_resource())
        actions.resources().unwrap()[:] = list(resources_set)

        self._raise_for_size()

    def _statements(self) -> '_PolicyDocumentStatementArray':
        return self['Statement']

    def _statement(self, sid: str) -> '_PolicyDocumentStatement':
        return self._statements().first_where(Sid=sid)

    def _resource_arn_from_prefix(self, prefix: str) -> str:
        return f'{self.bucket_arn()}/{prefix}/*'

    def _prefixes_chain_from_prefix(self, prefix: str) -> t.List[str]:
        """
        Returns slices of the prefix.

        For example, the prefix `'/a/b/c'` will be converted into the list `['/a/', '/a/b/', '/a/b/c/*']`.
        """
        components = prefix.split('/')
        prefixes = [
            '/'.join(components[:index+1]) + '/' for index in range(len(components))
        ]
        prefixes[-1] = prefixes[-1] + '*'
        return prefixes

    def _null_resource(self):
        return _null_object(self.bucket_arn())


class _PolicyDocumentStatement(_PolicyDocumentDict):
    def resources(self) -> '_PolicyDocumentStringArray':
        return self['Resource']

    def prefixes_condition(self) -> '_PolicyDocumentStringArray':
        return _PolicyDocumentStringArray(self._data['Condition']['StringLike']['s3:prefix'])


_PolicyDocumentStringArray = _PolicyDocumentArray[str, str]
_PolicyDocumentStatementArray = _PolicyDocumentArray[_PolicyDocumentStatement, dict]


def _new_policy_document_object(bucket_name: str, handle: str) -> SharingPolicyDocument:
    bucket_arn = f'arn:aws:s3:::{bucket_name}'
    return SharingPolicyDocument({
        'Version': '2012-10-17',
        'Statement': [
            {
                'Sid': _PACKAGES_LISTING_SID,
                'Action': ['s3:ListBucket'],
                'Effect': 'Allow',
                'Resource': [bucket_arn],
                'Condition': {
                    'StringLike': {
                        # add the package prefix. typically this is the package path prefix
                        # i.e. {org_name}/ and {org_name}/{package_name}/*.
                        # all of the prefixes leading to the package prefix must be included,
                        # this is because users need to be able to list the content of
                        # the prefix to be able to reach the package through the console.
                        's3:prefix': [''],
                        's3:delimiter': ['/'],
                    }
                }
            },
            {
                'Sid': _PACKAGES_ACTIONS_SID,
                'Effect': 'Allow',
                'Action': ['s3:*'],
                'Resource': [
                    # Since you can't have a statement with no resources, we add
                    # a dummy resource name for an object that does not exist.
                    _null_object(bucket_arn),
                    # add the resources shared. typically this is the bucket arn with the
                    # package path prefix and /* ({org_name}/{package_name}/*) to allow
                    # access to all resources within the package.
                ],
            },
        ]
    }, handle)

def _null_object(bucket_arn: str) -> str:
    return f'{bucket_arn}/__null_object__'

_BUCKETS_LISTING_STATEMENT = {
    'Sid': _BUCKETS_LISTING_SID,
    'Action': ['s3:ListAllMyBuckets', 's3:GetBucketLocation'],
    'Effect': 'Allow',
    'Resource': ['arn:aws:s3:::*'],
}
