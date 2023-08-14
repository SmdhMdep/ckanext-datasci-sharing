import logging

from .sharing_policy_document import SharingPolicyDocument
from .model import PackageSharingPolicy


logger = logging.getLogger(__name__)


class SharingPolicyRecord:
    # TODO remove me
    def __init__(self, package_prefix: str, package_policy: PackageSharingPolicy):
        self._package_prefix = package_prefix
        self._policy = package_policy
        self._handle = self._policy.handle
        self._allowed = self._policy.allowed
        self._recorded_allowed = self._allowed

    @property
    def handle(self) -> str:
        return self._handle

    @handle.setter
    def handle(self, value):
        self._handle = value

    def update(self, allow: bool):
        """Record an update to the policy for the package."""
        self._recorded_allowed = allow

    def has_changes(self) -> bool:
        return self._recorded_allowed != self._allowed

    def apply(self, document: SharingPolicyDocument):
        """Apply recorded changes to the policy to the document passed as argument."""
        document.update_prefix(self._package_prefix, self._recorded_allowed)

        self._allowed = self._recorded_allowed
        self._handle = document.handle
        self._policy.allowed = self._allowed
        self._policy.handle = self._handle
