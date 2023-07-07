import logging

import ckan.plugins as plugins
import ckan.plugins.toolkit as toolkit

from .sharing_policy_dataset_form import SharingPolicyDatasetForm


logger = logging.getLogger(__name__)


class DatasciSharingPlugin(plugins.SingletonPlugin, SharingPolicyDatasetForm):
    plugins.implements(plugins.IConfigurer)
    plugins.implements(plugins.IDatasetForm)

    # IConfigurer

    def update_config(self, config_):
        toolkit.add_template_directory(config_, 'templates')
        toolkit.add_public_directory(config_, 'public')
        toolkit.add_resource('fanstatic', 'datasci_sharing')
