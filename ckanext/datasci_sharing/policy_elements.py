from collections import abc
import json


def wrap_policy_document(value):
    if isinstance(value, dict):
        return PolicyDocumentObject(value)
    if isinstance(value, list):
        return PolicyDocumentArray(value)
    return value


class PolicyDocumentSizeLimitExceeded(Exception):
    _POLICY_DOCUMENT_SIZE_LIMIT = 6 * 1024 # 6 KB

    def __init__(self):
        super().__init__(f"policy document size limit exceeded")

    @classmethod
    def check(cls, document: 'PolicyDocumentObject'):
        if document.size() > cls._POLICY_DOCUMENT_SIZE_LIMIT:
            raise PolicyDocumentSizeLimitExceeded()


class PolicyDocumentBase(abc.Sized):
    __slots__ = ('_data',)

    def __init__(self, data):
        self._data = data

    def unwrap(self):
        return self._data

    def size(self):
        """Returns the count of characters within the document excluding whitespace."""
        return len(json.dumps(self._data, separators=(',', ':')))

    def raise_for_size(self):
        """Ensures that the document size limit has not been exceeded."""
        PolicyDocumentSizeLimitExceeded.check(self)

    def as_json(self):
        return json.dumps(self._data)

    def __len__(self):
        return len(self._data)

    def __repr__(self):
        return f"<{type(self).__name__} {self._data}>"


class PolicyDocumentObject(abc.Mapping, PolicyDocumentBase):
    LISTING_SID = 'AllowListingOfSharedPackages'
    ACTIONS_SID = 'AllowAllS3ActionsInSharedPackages'

    def __init__(self, data):
        super().__init__(data)

    @staticmethod
    def new(bucket_name) -> 'PolicyDocumentObject':
        return _new_policy_document_object(bucket_name)

    def __getitem__(self, key):
        return wrap_policy_document(self._data[key])

    def __iter__(self):
        return iter(self._data)

    def __getattr__(self, attr):
        attr_camel_case = _snake_to_camel_case(attr)
        try:
            return self[attr_camel_case]
        except KeyError:
            raise AttributeError(
                f"'{type(self).__name__}' object has no attribute '{attr}' ('{attr_camel_case}')"
            ) from None

class PolicyDocumentArray(abc.Sequence, PolicyDocumentBase):
    def __init__(self, data):
        super().__init__(data)

    def first_where(self, **kwargs):
        if len(kwargs) != 1:
            raise ValueError('cannot filter by more than one property')
        [(key, value)] = kwargs.items()
        return next(
            element for element in self
            if getattr(element, key) == value
        )

    def single(self):
        if len(self) != 1:
            raise ValueError("expected a single element")
        return self[0]

    def __getitem__(self, index):
        return wrap_policy_document(self._data[index])

    def __iter__(self):
        for item in self._data:
            yield wrap_policy_document(item)


def _new_policy_document_object(bucket_name: str) -> PolicyDocumentObject :
    bucket_arn = f'arn:aws:s3:::{bucket_name}'
    return wrap_policy_document({
        'Version': '2012-10-17',
        'Statement': [
            {
                'Sid': 'AllowUserToSeeBucketListInTheConsole',
                'Action': ['s3:ListAllMyBuckets', 's3:GetBucketLocation'],
                'Effect': 'Allow',
                'Resource': ['arn:aws:s3:::*']
            },
            {
                'Sid': PolicyDocumentObject.LISTING_SID,
                'Action': ['s3:ListBucket'],
                'Effect': 'Allow',
                'Resource': [bucket_arn],
                'Condition': {
                    'StringEquals': {
                        # add the package prefix. typically this is the package path prefix
                        # i.e. {org_name} and {org_name}/{package_name}.
                        # all of the prefixes leading to the package prefix must be included,
                        # this is because users need to be able to list the content of
                        # the prefix to be able to reach the package through the console.
                        's3:prefix': [''],
                    }
                }
            },
            {
                'Sid': PolicyDocumentObject.ACTIONS_SID,
                'Effect': 'Allow',
                'Action': ['s3:*'],
                'Resource': [
                    # Since you can't have a statement with no resources, we add
                    # a dummy resource name for an object that does not exist.
                    f'{bucket_arn}/__null_object__',
                    # add the resources shared. typically this is the bucket arn with the
                    # package path prefix and /* ({org_name}/{package_name}/*) to allow
                    # access to all resources within the package.
                ],
            },
        ]
    })


def _snake_to_camel_case(attr: str):
    result = []
    should_convert_next_to_uppercase = True
    for letter in attr:
        if letter == '_':
            should_convert_next_to_uppercase = True
        elif should_convert_next_to_uppercase:
            result.append(letter.upper())
            should_convert_next_to_uppercase = False
        else:
            result.append(letter)
    return ''.join(result)
