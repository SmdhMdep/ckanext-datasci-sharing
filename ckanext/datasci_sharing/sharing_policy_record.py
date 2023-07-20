from typing import List, Tuple
import os

from .sharing_policy_document import SharingPolicyDocument


class SharingPolicyRecord:
    def __init__(self):
        # Array of tuples, each tuple contains package and
        # boolean value of whether to share or not
        self._policy = []

    def update(self, pkg_dict: dict, allow: bool):
        """Record an update to the policy for the given package."""
        prefix = self._package_prefix(pkg_dict)
        self._policy.append((prefix, allow))

    def apply(self, document: SharingPolicyDocument) -> List[Tuple[str, bool]]:
        """Apply recorded changes to the policy to the document passed as argument."""
        for prefix, allow in self._policy:
            document.update_prefix(prefix, allow)

    def _package_prefix(self, pkg_dict: dict):
        """Returns a bucket prefix for the given package."""
        # TODO convert this into a strategy (injected through the plugins system)
        # so that multiple plugins can share the same strategy.
        return os.path.join(
            pkg_dict['organization']['name'],
            pkg_dict['name'],
        )
