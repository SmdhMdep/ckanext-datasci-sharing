import logging
from typing import TypedDict

from ckan.plugins import toolkit

from .config import config
from .sharing_policy_repository import SharingPolicyRepository


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
    with repo.sharing_policy() as policy:
        policy.update(package, allow=data['share'])
    logger.debug('completed updating policy document')
