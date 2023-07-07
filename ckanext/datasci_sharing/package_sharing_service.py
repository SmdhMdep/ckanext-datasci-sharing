import logging
import os

from .config import config
from .distributed_lock import distributed_lock
from .sharing_policy_repository import SharingPolicyRepository
from .sharing_policy_writer import SharingPolicyWriter


logger = logging.getLogger(__name__)


class PackageSharingService:
    def __init__(self):
        self._repo = SharingPolicyRepository(config.group_name, config.bucket_name)

    def share_package(self, pkg_dict: str):
        self._update_package_sharing_policy(pkg_dict, share=True)

    def unshare_package(self, pkg_dict: dict):
        self._update_package_sharing_policy(pkg_dict, share=False)

    def _update_package_sharing_policy(self, pkg_dict: dict, *, share: bool):
        logger.debug(
            f'updating package %s sharing policy to %s',
            pkg_dict['name'], 'allow' if share else 'deny'
        )
        for document in self._repo.policy_document():
            writer = SharingPolicyWriter(document=document)
            writer.update_prefix_policy(_package_prefix(pkg_dict), allow=share)
        logger.debug('completed updating policy document')


def _package_prefix(pkg_dict: dict):
    """Returns a bucket prefix for the given package."""
    # TODO convert this into a strategy (injected through the plugins system)
    # or an action so that multiple plugins can share the same strategy.
    return os.path.join(
        pkg_dict['organization']['name'],
        pkg_dict['name'],
    )
