from ast import literal_eval

from ckan.plugins.toolkit import config as ckan_config


SHARE_INTERNALLY_FIELD = 'share_internally'


class Config:
    @property
    def sharing_policy_arn(self) -> str:
        return ckan_config['ckanext.datasci_sharing.policy_arn']

    @property
    def aws_session_options(self) -> dict:
        options = self._aws_session_options()
        options.pop('aws_access_key_id', None)
        options.pop('aws_secret_access_key', None)
        return options

    @property
    def aws_access_key_id(self) -> str:
        return (
            ckan_config.get('ckanext.datasci_sharing.aws_access_key_id')
            or self._aws_session_options().get('aws_access_key_id')
        )

    @property
    def aws_secret_access_key(self) -> str:
        return (
            ckan_config.get('ckanext.datasci_sharing.aws_secret_access_key')
            or self._aws_session_options().get('aws_secret_access_key')
        )

    def _aws_session_options(self) -> dict:
        return literal_eval(ckan_config.get(
            'ckanext.datasci_sharing.aws_session_options', '{}'
        ))


config = Config()
