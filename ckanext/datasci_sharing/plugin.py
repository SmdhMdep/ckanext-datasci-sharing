import logging

import ckan.plugins as plugins
import ckan.plugins.toolkit as toolkit

from .auth import share_internally_show, share_internally_update
from .actions import sync_package_sharing_policy
from .config import SHARE_INTERNALLY_FIELD
from .sharing_policy_dataset_form import SharingPolicyDatasetForm


logger = logging.getLogger(__name__)


class DatasciSharingPlugin(plugins.SingletonPlugin, SharingPolicyDatasetForm):
    plugins.implements(plugins.IConfigurer)
    plugins.implements(plugins.IAuthFunctions)
    plugins.implements(plugins.IActions)
    plugins.implements(plugins.IDatasetForm)
    plugins.implements(plugins.IPackageController, inherit=True)

    # IConfigurer

    def update_config(self, config_):
        toolkit.add_template_directory(config_, 'templates')
        toolkit.add_public_directory(config_, 'public')
        toolkit.add_resource('fanstatic', 'datasci_sharing')

    # IAuthFunctions

    def get_auth_functions(self):
        return {
            share_internally_show.__name__: share_internally_show,
            share_internally_update.__name__: share_internally_update,
        }

    # IActions

    def get_actions(self):
        return {sync_package_sharing_policy.__name__: sync_package_sharing_policy}

    # IPackageController

    def after_show(self, context, pkg_dict):
        try:
            toolkit.check_access('share_internally_show', context, pkg_dict)
            pkg_dict.setdefault(SHARE_INTERNALLY_FIELD, False)
        except toolkit.NotAuthorized:
            pkg_dict.pop(SHARE_INTERNALLY_FIELD, None)

        return pkg_dict

    def _update_policy(self, context, pkg_dict, delete=False):
        sync_package_sharing_policy(context, {
            'package_id': pkg_dict['id'],
            'share': _is_sharing_internally(pkg_dict) and not delete
        })

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
