from contextlib import contextmanager
import copy
import functools
from typing import Any, Tuple, Optional, Iterator

import boto3
import boto3.session

from .config import config
from .model import PackageSharingPolicy
from .distributed_lock import distributed_lock
from .sharing_policy_document import PolicyDocumentSizeLimitExceeded, SharingPolicyDocument
from .sharing_policy_record import SharingPolicyRecord


Policy = Any # Type alias for the policy resource from boto3
Group = Any # Type alias for the group resource from boto3

_GROUP_PATH_PREFIX = "/smdh-aep/datasci-sharing-groups/"
_GROUP_POLICIES_LIMIT = 10


def retry(stop_after: int):
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            last_exc = None
            for _ in range(stop_after):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    last_exc = e
            else:
                raise last_exc
        return wrapper
    return decorator

def _create_boto3_session():
    return boto3.Session(
        aws_access_key_id=config.aws_access_key_id,
        aws_secret_access_key=config.aws_secret_access_key,
        **config.aws_session_options,
    )

def _handle_from_sequences(g_seq: int, p_seq: int) -> str:
    return ':'.join(map(str, (g_seq, p_seq)))

def _handle_to_sequences(handle: Optional[str]) -> Tuple[Optional[int], Optional[int]]:
    return tuple(map(int, handle.split(':'))) if handle is not None else (None, None)

def _policy_seq(policy: Optional[Policy]) -> int:
    return -1 if policy is None else int(policy.policy_name[-1])

def _group_seq(name_prefix: str, group: Optional[Group]):
    return -1 if group is None else int(group.name.strip(name_prefix))


class SharingPolicyRepository:
    def __init__(self, resources_prefix: str, bucket_name: str):
        self.group_name_prefix = f"{resources_prefix}Group"
        self._bucket_name = bucket_name
        self._session = _create_boto3_session()
        self._resource = self._session.resource('iam')

    def _get_group(self, sequence: Optional[int]) -> Tuple[int, Optional[Group]]:
        """Returns the group that corresponds to the sequence. If sequence is None then returns
        the last group.
        """
        if sequence is not None:
            name = f'{self.group_name_prefix}{sequence}'
            return sequence, self._resource.Group(name)
        else:
            latest_sequence, latest_group = -1, None
            groups = self._resource.groups.filter(PathPrefix=_GROUP_PATH_PREFIX)
            for group in groups:
                # all groups end with a number
                sequence = int(group.name.strip(self.group_name_prefix))
                if latest_sequence < sequence:
                    latest_group = group
            return latest_sequence, latest_group

    def _get_policy(self, group: Optional[Group], sequence: Optional[int]) -> Optional[Policy]:
        """Returns the policy attached to the group and which corresponds to the sequence. If sequence is None then returns
        the last policy within the group.
        """
        if group is None:
            return -1, None
        elif sequence is not None:
            base_arn = group.arn.rpartition(":")[0]
            arn = f'{base_arn}:policy/{group.name}Policy{sequence}'
            return sequence, self._resource.Policy(arn)
        else:
            latest_sequence, latest_policy = -1, None
            for policy in group.attached_policies.all():
                # all policies end in the pattern {GroupName}Policy{0-9}
                sequence = int(policy.arn[-1])
                if latest_sequence < sequence:
                    latest_sequence, latest_policy = sequence, policy

            return latest_sequence, latest_policy

    def _create_group(self, prev_group: Optional[Group]) -> Group:
        sequence = _group_seq(prev_group) + 1
        group = self._resource.create_group(
            GroupName=f"{self.group_name_prefix}{sequence}",
            Path=_GROUP_PATH_PREFIX,
        )
        if prev_group is not None:
            for user in prev_group.users.all():
                group.add_user(user.name)
        return group

    def _create_policy(self, group: Group, p_seq: int, document: SharingPolicyDocument) -> Policy:
        g_seq = _group_seq(self.group_name_prefix, group)
        policy = self._resource.create_policy(
            PolicyName=f'{self.group_name_prefix}{g_seq}Policy{p_seq}',
            PolicyDocument=document.as_json(),
        )
        group.attach_policy(PolicyArn=policy.arn)
        return policy

    def _update_policy(self, policy: Policy, document: SharingPolicyDocument):
        old_version = policy.default_version
        policy.create_version(PolicyDocument=document.as_json(), SetAsDefault=True)
        old_version.delete()

    def _create_or_update_sharing_policy(
            self,
            group: Optional[Group],
            policy: Optional[Policy],
            record: SharingPolicyRecord,
            is_policy_full=False,
        ):
        g_seq, p_seq = _group_seq(self.group_name_prefix, group), _policy_seq(policy)
        should_create_policy = policy is None or is_policy_full
        should_create_group = group is None or should_create_policy and p_seq + 1 == _GROUP_POLICIES_LIMIT

        # update the sequence numbers to the group and policy numbers we're going to update
        if should_create_group:
            g_seq += 1
            p_seq = -1
        if should_create_policy:
            p_seq += 1

        handle = _handle_from_sequences(g_seq, p_seq)
        document = (
            SharingPolicyDocument.new(self._bucket_name, handle) if should_create_policy
            else SharingPolicyDocument(copy.deepcopy(policy.default_version.document), handle)
        )

        is_first_policy = group is None and policy is None
        if is_first_policy:
            document.allow_buckets_listing()
        record.apply(document)

        if should_create_group:
            group = self._create_group(group)

        if should_create_policy:
            self._create_policy(group, p_seq, document)
        else:
            self._update_policy(policy, document)

    @contextmanager
    def sharing_policy(self, package_id: str, package_prefix: str) -> Iterator[SharingPolicyRecord]:
        package_policy = PackageSharingPolicy.get_or_default(package_id, for_update=True)
        record = SharingPolicyRecord(package_prefix, package_policy)
        yield record
        if record.has_changes():
            self.do_sharing_policy_update(record, record.handle)
        package_policy.save()

    @retry(stop_after=1)
    def do_sharing_policy_update(self, record: SharingPolicyRecord, handle: Optional[str]):
        with distributed_lock(type(self).__name__, timeout=2) as lock:
            g_seq, p_seq = _handle_to_sequences(handle)
            g_seq, group = self._get_group(g_seq)
            p_seq, policy = self._get_policy(group, p_seq)
            try:
                lock.extend(1)
                self._create_or_update_sharing_policy(group, policy, record)
            except PolicyDocumentSizeLimitExceeded:
                if policy is None:
                    raise
                lock.extend(1)
                self._create_or_update_sharing_policy(group, policy, record, is_policy_full=True)