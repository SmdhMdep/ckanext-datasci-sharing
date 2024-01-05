import typing as t
import logging
import json


logger = logging.getLogger(__name__)


class _ListExt:
    """List extensions to support traversal of lists within a policy document."""
    @staticmethod
    def first_where(array: t.List[t.Dict], **filter) -> t.Dict:
        if len(filter) != 1:
            raise ValueError('cannot filter by more than one property')

        [(key, value)] = filter.items()
        return next(element for element in array if element[key] == value)

    @staticmethod
    def single(value: t.Union[str, list]) -> t.Any:
        if isinstance(value, str):
            return value
        elif len(value) != 1:
            raise ValueError("expected a single element")
        else:
            return value[0]

    @staticmethod
    def as_list(value: t.Union[str, list]) -> list:
        """
        Safely access a value that can be either a list of strings or a string as a list.

        This is useful because AWS policy elements allows a value to be either a list or a single value,
        even if that value was previously written as a list.
        """
        if isinstance(value, str):
            return [value]
        return value


class PolicyDocumentSizeLimitExceeded(Exception):
    _POLICY_DOCUMENT_SIZE_LIMIT = 20 * 1024  # 20 KB

    def __init__(self):
        super().__init__(f"policy document size limit exceeded")

    @classmethod
    def check(cls, document: 'SharingPolicyDocument'):
        if document.size() > cls._POLICY_DOCUMENT_SIZE_LIMIT:
            raise PolicyDocumentSizeLimitExceeded()


_PACKAGES_LISTING_SID = 'A'
_PACKAGES_ACTIONS_SID = 'B'


class SharingPolicyDocument:
    """A wrapper for the policy document of an access point providing operations
    for adding shared prefixes to the policy.
    """
    __slots__ = ('_document', 'access_point_name')

    def __init__(self, document, access_point_name):
        self._document = document
        self.access_point_name = access_point_name

    @classmethod
    def new(cls, region: str, account_id: str, access_point_name: str) -> 'SharingPolicyDocument':
        document = _new_policy_document(region, account_id, access_point_name)
        return cls(document, access_point_name)

    def _statements(self):
        return self._document['Statement']

    def _statement(self, sid: str):
        return _ListExt.first_where(self._statements(), Sid=sid)

    def access_point_arn(self) -> str:
        """The root resource ARN. This is the ARN of the resource this policy is attached to."""
        return _ListExt.single(self._statement(_PACKAGES_LISTING_SID)['Resource'])

    def size(self) -> int:
        """Returns the count of characters within the document excluding whitespace."""
        return len(json.dumps(self._document, separators=(',', ':')))

    def as_json(self):
        return json.dumps(self._document)

    def update_prefix(self, prefix: str, allow: bool):
        listing_stmt = self._statement(_PACKAGES_LISTING_SID)
        prefixes_set = set(_ListExt.as_list(listing_stmt['Condition']['StringLike']['s3:prefix']))
        prefixes_chain = self._prefixes_chain_from_prefix(prefix)
        if allow:
            prefixes_set.update(prefixes_chain)
        else:
            # only remove the prefix for the package itself because prefixes
            # before the package may be common with other shared packages
            prefixes_set.difference_update(prefixes_chain[-1:])
        listing_stmt['Condition']['StringLike']['s3:prefix'] = list(prefixes_set)

        actions_stmt = self._statement(_PACKAGES_ACTIONS_SID)
        resources_set = set(_ListExt.as_list(actions_stmt['Resource']))
        if allow:
            resources_set.add(self._prefix_objects_arn(prefix))
            resources_set.discard(_null_object(self.access_point_arn()))
        else:
            resources_set.discard(self._prefix_objects_arn(prefix))
            if not resources_set:
                resources_set.add(_null_object(self.access_point_arn()))
        actions_stmt['Resource'] = list(resources_set)

        logger.debug(json.dumps(self._document, indent=4))

        PolicyDocumentSizeLimitExceeded.check(self)

    def _prefix_objects_arn(self, prefix: str) -> str:
        """ARN for all objects within the provided `prefix`"""
        return f'{self.access_point_arn()}/object/{prefix}/*'

    def _prefixes_chain_from_prefix(self, prefix: str) -> t.List[str]:
        """
        Convert a prefix into a traversable chain of prefixes.

        For example, the prefix `'/a/b/c'` will be converted into the list `['/a/', '/a/b/', '/a/b/c/*']`.
        """
        components = prefix.split('/')
        prefixes = [
            '/'.join(components[:index+1]) + '/' for index in range(len(components))
        ]
        prefixes[-1] = prefixes[-1] + '*'
        return prefixes


_ALLOWED_PRINCIPAL_PATTERNS = [
    "arn:aws:iam::{account_id}:role/*DataScientistRole",
    "arn:aws:iam::{account_id}:role/service-role/*DataScientistServiceRole",
    "arn:aws:iam::{account_id}:role/service-role/*",
    "arn:aws:iam::{account_id}:role/aws-service-role/*",
    "arn:aws:iam::{account_id}:user/data-scientist/*",
]


def _new_policy_document(region: str, account_id: str, access_point_name: str) -> dict:
    access_point_arn = f'arn:aws:s3:{region}:{account_id}:accesspoint/{access_point_name}'
    principal_arns = [
        principal.format(account_id=account_id)
        for principal in _ALLOWED_PRINCIPAL_PATTERNS
    ]

    return {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Sid": _PACKAGES_LISTING_SID,
                "Principal": {"AWS": "*"},
                "Effect": "Allow",
                "Action": ["s3:ListBucket"],
                "Resource": access_point_arn,
                "Condition": {
                    "StringLike": {
                        "s3:prefix": [""],
                        "s3:delimiter": ["/"],
                    },
                    "ArnLike": {
                        "aws:PrincipalArn": principal_arns,
                    }
                },
            },
            {
                "Sid": _PACKAGES_ACTIONS_SID,
                "Principal": {"AWS": "*"},
                "Effect": "Allow",
                "Action": ["s3:Get*"],
                "Resource": [
                    # Since you can't have a statement with no resources, we add
                    # a dummy resource name for an object that does not exist.
                    _null_object(access_point_arn),
                    # add the resources shared. typically this is the bucket arn with the
                    # package path prefix and /* ({org_name}/{package_name}/*) to allow
                    # access to all resources within the package.
                ],
                "Condition": {
                    "ArnLike": {
                        "aws:PrincipalArn": principal_arns,
                    }
                }
            },
        ],
    }


def _null_object(access_point_arn: str) -> str:
    return f'{access_point_arn}/object/__null_object__'
