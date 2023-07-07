import logging
import os

import boto3
import boto3.session

from .config import config
from .distributed_lock import distributed_lock
from .sharing_policy_writer import SharingPolicyWriter
from .policy_elements import PolicyDocumentObject


logger = logging.getLogger(__name__)


def _create_boto3_session():
    return boto3.Session(
        aws_access_key_id=config.aws_access_key_id,
        aws_secret_access_key=config.aws_secret_access_key,
        **config.aws_session_options,
    )


class PackageSharingService:
    def __init__(self):
        self._session = _create_boto3_session()
        self._resource = self._session.resource('iam')
        self._policy_arn = config.sharing_policy_arn

    def share_package(self, pkg_dict: str):
        self._update_package_sharing_policy(pkg_dict, share=True)

    def unshare_package(self, pkg_dict: dict):
        self._update_package_sharing_policy(pkg_dict, share=False)

    def _update_package_sharing_policy(self, pkg_dict: dict, *, share: bool):
        logger.debug(f'updating package %s sharing policy to %s', pkg_dict['name'], 'allow' if share else 'deny')
        with distributed_lock(type(self).__name__):
            policy = self._resource.Policy(self._policy_arn)
            current_document = policy.default_version.document
            writer = SharingPolicyWriter(document=current_document)
            writer.update_prefix_policy(_package_prefix(pkg_dict), allow=share)
            updated_document = writer.close()
            self._update_policy(policy, updated_document)
        logger.debug('completed updating policy document')

    def _update_policy(self, policy, new_document: PolicyDocumentObject):
        old_version = policy.default_version
        logger.debug('creating new version for updated policy document')
        policy.create_version(PolicyDocument=new_document.as_json(), SetAsDefault=True)
        logger.debug('deleting old policy document version')
        old_version.delete()


def _package_prefix(pkg_dict: dict):
    """Returns a bucket prefix for the given package."""
    # TODO convert this into a strategy and inject it through the plugins system
    # so that multiple plugins can share the same strategy.
    return os.path.join(
        pkg_dict['organization']['name'],
        pkg_dict['name'],
    )