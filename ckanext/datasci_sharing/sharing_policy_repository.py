from contextlib import contextmanager
import logging
import json
from typing import Optional, Iterator

import boto3
import boto3.session
from botocore.exceptions import ClientError as BotoClientError

from .config import BucketConfig, config
from .model import PackageSharingPolicy
from .distributed_lock import distributed_lock
from .sharing_policy_document import SharingPolicyDocument
from .sharing_policy_record import SharingPolicyRecord
from .utils import retry


logger = logging.getLogger(__name__)


def _create_boto3_session():
    return boto3.Session(
        aws_access_key_id=config.aws_access_key_id,
        aws_secret_access_key=config.aws_secret_access_key,
        **config.aws_session_options,
    )


class SharingNotAvailable(Exception):
    """Exception raised when sharing is not available yet and should be retried
    at a later time.
    """
    pass


class ShortOrganizationNameStrategy:
    def __init__(self, session: boto3.Session):
        self._lambda = session.client('lambda', region_name='eu-west-1')

    def __call__(self, title: str) -> str:
        response = self._lambda.invoke(
            FunctionName='arn:aws:lambda:eu-west-1:450869586150:function:GetShortGroup',
            Payload=json.dumps({"group": title}).encode(),
        )
        error = response.get('FunctionError')
        payload = json.loads(response['Payload'].read().decode())

        if error or payload['statusCode'] != 200:
            logger.error(
                'error invoking GetShortName lambda function: %s: %s',
                error or 'client error',
                payload,
            )
            raise Exception('unable to get group short name')
        else:
            return payload['body']


class AccessPointService:
    def __init__(self, session: boto3.Session, account_id: str, bucket_name: str, bucket_region: str):
        self._s3_control = session.client('s3control', region_name=bucket_region)
        self.account_id = account_id
        self.bucket_name = bucket_name
        self.bucket_region = bucket_region

    def get_policy(self, name: str) -> Optional[dict]:
        try:
            response = self._s3_control.get_access_point_policy(AccountId=self.account_id, Name=name)
            policy = response['Policy']
            return json.loads(policy) if policy != '' else None
        except BotoClientError as e:
            if e.response['Error']['Code'] not in ['NoSuchAccessPoint', 'NoSuchAccessPointPolicy']:
                raise
            return None

    def create(self, name: str):
        try:
            self._s3_control.create_access_point(
                AccountId=self.account_id,
                Bucket=self.bucket_name,
                Name=name,
            )
        except BotoClientError as e:
            if e.response['Error']['Code'] != 'AccessPointAlreadyOwnedByYou':
                raise

    def update(self, name: str, policy: dict):
        try:
            self._update_with_retry(name, policy)
            return True
        except BotoClientError as e:
            if e.response['Error']['Code'] == 'NoSuchAccessPoint':
                # TODO verify that this is the case
                # can happen even if the access point was created previously,
                # due to AWS eventually consistent API behavior.
                return False
            raise

    @retry(2, backoff=2)
    def _update_with_retry(self, name: str, policy: dict):
        self._s3_control.put_access_point_policy(
            AccountId=self.account_id,
            Name=name,
            Policy=policy,
        )


class SharingPolicyRepository:
    def __init__(
            self,
            resources_prefix: str,
            bucket_config: BucketConfig,
        ):
        session = _create_boto3_session()
        self._ap_service = AccessPointService(
            session,
            bucket_config.account_id,
            bucket_config.bucket_name,
            bucket_config.bucket_region,
        )
        self._get_org_short_name = ShortOrganizationNameStrategy(session)
        self._access_point_prefix = resources_prefix

    def _get_or_create_document(self, name: str) -> SharingPolicyDocument:
        doc = self._ap_service.get_policy(name)
        if doc is not None:
            return SharingPolicyDocument(doc, name)

        self._ap_service.create(name)
        policy = SharingPolicyDocument.new(
            self._ap_service.bucket_region,
            self._ap_service.account_id,
            name,
        )
        return policy

    def _save_document(self, policy: SharingPolicyDocument):
        if not self._ap_service.update(policy.access_point_name, policy.as_json()):
            raise SharingNotAvailable()

    def _get_policy_record(self, package_id, package_prefix) -> SharingPolicyRecord:
        policy_entity = PackageSharingPolicy.get_or_default(package_id, for_update=True)
        record = SharingPolicyRecord(package_prefix, policy_entity)
        if record.handle and ":" in record.handle:
            record.handle = None
            record.allowed = False
        return record

    @contextmanager
    def sharing_policy(self, org_title: str, package_id: str, package_prefix: str) -> Iterator[PackageSharingPolicy]:
        policy = self._get_policy_record(package_id, package_prefix)

        yield policy

        if policy.has_changes():
            org_short_name = self._get_org_short_name(org_title)
            if not policy.handle:
                policy.handle = f'{self._access_point_prefix}-{org_short_name}'

            with distributed_lock(f'sharing_policy_repository.access_points.{policy.handle}'):
                document = self._get_or_create_document(policy.handle)
                document.update_prefix(package_prefix, policy.allowed)
                self._save_document(document)

            policy.save()
