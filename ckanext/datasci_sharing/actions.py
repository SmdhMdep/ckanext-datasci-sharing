import logging
import os
from typing import TypedDict

from ckan.plugins import toolkit

from .config import config
from .sharing_policy_repository import SharingPolicyRepository
from .sharing_policy_writer import SharingPolicyWriter


logger = logging.getLogger(__name__)


class SyncPackageSharingPolicyDataDict(TypedDict):
    package_id: dict
    share: bool


def sync_package_sharing_policy(context, data: SyncPackageSharingPolicyDataDict):
    package = toolkit.get_action('package_show')(context, {'id': data['package_id']})
    repo = SharingPolicyRepository(config.group_name, config.bucket_name)
    logger.debug(
        f'updating package %s sharing policy to %s',
        package['name'], 'allow' if data['share'] else 'deny'
    )
    for document in repo.policy_document():
        writer = SharingPolicyWriter(document=document)
        writer.update_prefix_policy(_package_prefix(package), allow=data['share'])
    logger.debug('completed updating policy document')


def _package_prefix(pkg_dict: dict):
    """Returns a bucket prefix for the given package."""
    # TODO convert this into a strategy (injected through the plugins system)
    # or an action so that multiple plugins can share the same strategy.
    return os.path.join(
        pkg_dict['organization']['name'],
        pkg_dict['name'],
    )
