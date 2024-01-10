from typing import TypedDict

from ckan.plugins import toolkit

from .config import config, SHARE_INTERNALLY_FIELD
from .sharing_policy_repository import SharingPolicyRepository


class SyncPackageSharingPolicyDataDict(TypedDict):
    package_id: str


def sync_package_sharing_policy(context, data: SyncPackageSharingPolicyDataDict):
    package_id = toolkit.get_or_bust(data, "package_id")
    show_package_data = {'id': package_id}

    toolkit.check_access('package_show', context, show_package_data)

    show_context = dict(context, for_update=True)
    package = toolkit.get_action('package_show')(show_context, show_package_data)

    toolkit.check_access('share_internally_update', context, package)
    toolkit.check_access('package_update', context, package)

    is_deleted = package.get("state") == "deleted"
    if is_deleted:
        package[SHARE_INTERNALLY_FIELD] = False

    allowed = package.get(SHARE_INTERNALLY_FIELD, False)
    org_title = package['organization']['title']
    prefix = toolkit.h['get_package_cloud_storage_key'](package)

    repo = SharingPolicyRepository(config.bucket.bucket_name, config.bucket)
    with repo.sharing_policy(org_title, package_id, prefix) as policy:
        policy.allowed = allowed
