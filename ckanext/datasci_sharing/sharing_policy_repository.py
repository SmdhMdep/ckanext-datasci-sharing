from contextlib import contextmanager
import copy
import logging
from typing import Any, Tuple, Optional, Iterator

import boto3
import boto3.session

from .config import config
from .distributed_lock import distributed_lock
from .sharing_policy_document import PolicyDocumentSizeLimitExceeded, SharingPolicyDocument
from .sharing_policy_record import SharingPolicyRecord


logger = logging.getLogger(__name__)

Policy = Any # Type alias for the policy object from boto3


def _create_boto3_session():
    return boto3.Session(
        aws_access_key_id=config.aws_access_key_id,
        aws_secret_access_key=config.aws_secret_access_key,
        **config.aws_session_options,
    )


class SharingPolicyRepository:
    def __init__(self, group_name: str, bucket_name: str):
        self._group_name = group_name
        self._bucket_name = bucket_name
        self._session = _create_boto3_session()
        self._resource = self._session.resource('iam')

    def _get_latest_policy(self) -> Tuple[int, Policy]:
        policies = self._resource.Group(self._group_name).attached_policies.all()
        latest_sequence, latest_policy = -1, None
        for policy in policies:
            # all policies end in the pattern Policy{0-9}
            sequence = int(policy.arn[-1])
            if latest_sequence < sequence:
                latest_sequence, latest_policy = sequence, policy
        return (latest_sequence, latest_policy)

    def _create_policy(self, sequence, document: SharingPolicyDocument):
        policy = self._resource.create_policy(
            PolicyName=f'{self._group_name}Policy{sequence}',
            PolicyDocument=document.as_json(),
        )
        self._resource.Group(self._group_name).attach_policy(PolicyArn=policy.arn)

    def _update_policy(self, policy: Policy, document: SharingPolicyDocument):
        old_version = policy.default_version
        logger.debug('creating new version for updated policy document')
        policy.create_version(PolicyDocument=document.as_json(), SetAsDefault=True)
        logger.debug('deleting old policy document version')
        old_version.delete()

    def _upsert(self, sequence: int, policy: Optional[Policy], record: SharingPolicyRecord):
        document = (
            SharingPolicyDocument.new(self._bucket_name) if policy is None
            else SharingPolicyDocument(copy.deepcopy(policy.default_version.document))
        )
        if sequence == -1:
            document.allow_buckets_listing()
        record.apply(document)

        if policy is None:
            self._create_policy(sequence, document)
        else:
            self._update_policy(policy, document)

    @contextmanager
    def sharing_policy(self) -> Iterator[SharingPolicyRecord]:
        record = SharingPolicyRecord()
        yield record
        with distributed_lock(type(self).__name__):
            (sequence, policy) = self._get_latest_policy()
            if policy is not None:
                raw_document = policy.default_version.document
                document = SharingPolicyDocument(copy.deepcopy(raw_document))
                try:
                    self._upsert(sequence, policy, record)
                except PolicyDocumentSizeLimitExceeded:
                    policy = None
            # can be set to None in the previous if statement
            if policy is None:
                self._upsert(sequence, policy, record)
