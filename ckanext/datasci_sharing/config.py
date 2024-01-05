from ast import literal_eval
from typing import NamedTuple, Optional

from ckan.plugins.toolkit import config as ckan_config


SHARE_INTERNALLY_FIELD = 'share_internally'


class BucketConfig(NamedTuple):
    account_id: str
    bucket_region: str
    bucket_name: str


class Config:
    @property
    def iam_resources_prefix(self) -> str:
        return ckan_config['ckanext.datasci_sharing.iam_resources_prefix']

    @property
    def bucket(self) -> BucketConfig:
        return BucketConfig(
            account_id=config.aws_account_id,
            bucket_region=config.bucket_region,
            bucket_name=config.bucket_name,
        )

    @property
    def bucket_name(self) -> str:
        return ckan_config['ckanext.datasci_sharing.bucket_name']

    @property
    def bucket_region(self) -> str:
        return ckan_config['ckanext.datasci_sharing.bucket_region']

    @property
    def aws_account_id(self) -> str:
        return ckan_config['ckanext.datasci_sharing.aws_account_id']

    @property
    def aws_session_options(self) -> dict:
        options = self._aws_session_options()
        options.pop('aws_access_key_id', None)
        options.pop('aws_secret_access_key', None)
        return options

    @property
    def aws_access_key_id(self) -> Optional[str]:
        return _not_empty_or_none(
            ckan_config.get('ckanext.datasci_sharing.aws_access_key_id')
            or self._aws_session_options().get('aws_access_key_id')
        )

    @property
    def aws_secret_access_key(self) -> Optional[str]:
        return _not_empty_or_none(
            ckan_config.get('ckanext.datasci_sharing.aws_secret_access_key')
            or self._aws_session_options().get('aws_secret_access_key')
        )

    def _aws_session_options(self) -> dict:
        return literal_eval(ckan_config.get(
            'ckanext.datasci_sharing.aws_session_options', '{}'
        ))


def _not_empty_or_none(value):
    return value if value else None


config = Config()
