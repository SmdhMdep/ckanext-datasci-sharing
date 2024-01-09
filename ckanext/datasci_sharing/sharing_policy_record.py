from .model import PackageSharingPolicy


class SharingPolicyRecord(PackageSharingPolicy):
    def __init__(self, package_prefix: str, package_policy: PackageSharingPolicy):
        self._package_prefix = package_prefix
        self._policy = package_policy
        self._init_allowed = self._policy.allowed

    def __getattr__(self, name):
        return getattr(self._policy, name)

    @property
    def handle(self):
        return self._policy.handle

    @handle.setter
    def handle(self, value):
        self._policy.handle = value

    @property
    def allowed(self):
        return self._policy.allowed

    @allowed.setter
    def allowed(self, value):
        self._policy.allowed = value

    def has_changes(self) -> bool:
        return self._init_allowed != self.allowed
