import logging

import ckan.plugins as plugins
import ckan.plugins.toolkit as toolkit

from .auth import share_internally_show
from .config import SHARE_INTERNALLY_FIELD
from .package_sharing_service import PackageSharingService
from .sharing_policy_dataset_form import SharingPolicyDatasetForm


logger = logging.getLogger(__name__)


class DatasciSharingPlugin(plugins.SingletonPlugin, SharingPolicyDatasetForm):
    plugins.implements(plugins.IConfigurer)
    plugins.implements(plugins.IAuthFunctions)
    plugins.implements(plugins.IDatasetForm)
    plugins.implements(plugins.IPackageController, inherit=True)

    # IConfigurer

    def update_config(self, config_):
        toolkit.add_template_directory(config_, 'templates')
        toolkit.add_public_directory(config_, 'public')
        toolkit.add_resource('fanstatic', 'datasci_sharing')

    # IAuthFunctions

    def get_auth_functions(self):
        return {'share_internally_show': share_internally_show}

    # IPackageController

    def after_show(self, context, pkg_dict):
        try:
            toolkit.check_access('share_internally_show', context, pkg_dict)
        except toolkit.NotAuthorized:
            pkg_dict.pop(SHARE_INTERNALLY_FIELD)

        return pkg_dict

    def _update_policy(self, context, pkg_dict, delete=False):
        if "organization" not in pkg_dict:
            organization_show = toolkit.get_action("organization_show")
            pkg_dict["organization"] = organization_show(context, {
                "id": pkg_dict["owner_org"]
            })

        service = PackageSharingService()
        if _is_sharing_internally(pkg_dict) and not delete:
            service.share_package(pkg_dict)
        else:
            service.unshare_package(pkg_dict)

    def after_create(self, context, pkg_dict):
        self._update_policy(context, pkg_dict)

    def after_update(self, context, pkg_dict):
        self._update_policy(context, pkg_dict)

    def after_delete(self, context, pkg_dict):
        self._update_policy(context, pkg_dict, delete=True)


def _is_sharing_internally(pkg_dict: dict):
    for extra in pkg_dict['extras']:
        if extra['key'] == SHARE_INTERNALLY_FIELD:
            return extra['value']
    return False
