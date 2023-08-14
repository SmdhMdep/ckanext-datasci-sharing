import os
from typing import TypedDict

from ckan.plugins import toolkit

from .config import config, SHARE_INTERNALLY_FIELD
from .sharing_policy_repository import SharingPolicyRepository


class SyncPackageSharingPolicyDataDict(TypedDict):
    package_id: dict


def sync_package_sharing_policy(context, data: SyncPackageSharingPolicyDataDict):
    # lock the package for this update
    package_id = toolkit.get_or_bust(data, "package_id")
    show_package_data = {'id': package_id}

    toolkit.check_access('package_show', context, show_package_data)
    package = toolkit.get_action('package_show')(dict(context, for_update=True), show_package_data)

    toolkit.check_access('share_internally_update', context, package)
    toolkit.check_access('package_update', context, package)

    is_deleted = package.get("state") == "deleted"
    if is_deleted:
        package[SHARE_INTERNALLY_FIELD] = False

    allow = package.get(SHARE_INTERNALLY_FIELD, False)
    prefix = _package_prefix(package)
    repo = SharingPolicyRepository(config.iam_resources_prefix, config.bucket_name)
    with repo.sharing_policy(package_id, prefix) as policy:
        policy.update(allow)

def _package_prefix(package):
    """Returns a bucket prefix for the given package."""
    # TODO convert this into a strategy (injected through the plugins system)
    # so that multiple plugins can share the same strategy.
    return os.path.join(
        '1',
        package['organization']['name'],
        package['name'],
    )
