from .sharing_policy_document import SharingPolicyDocument
from .model import PackageSharingPolicy


class SharingPolicyRecord:
    def __init__(self, package_prefix: str, package_policy: PackageSharingPolicy):
        self._package_prefix = package_prefix
        self._policy = package_policy
        self._recorded_allowed = self._policy.allowed

    @property
    def handle(self) -> str:
        return self._policy.handle

    def update(self, allow: bool):
        """Record an update to the policy for the package."""
        self._recorded_allowed = allow

    def has_changes(self) -> bool:
        return self._recorded_allowed != self._policy.allowed

    def apply(self, document: SharingPolicyDocument):
        """Apply recorded changes to the policy to the document passed as argument."""
        document.update_prefix(self._package_prefix, self._recorded_allowed)

        self._policy.allowed = self._recorded_allowed
        self._policy.handle = document.handle
