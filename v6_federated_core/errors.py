from typing import Any, Dict, Optional

from .types import FailureCategory, FailureDetail


class FederatedCoreError(Exception):
    category = FailureCategory.EXECUTION
    owner = "algorithm"
    retryable = False

    def __init__(self, message: str, *, meta: Optional[Dict[str, Any]] = None):
        super().__init__(message)
        self.meta = meta or {}

    def to_failure_detail(self) -> FailureDetail:
        return FailureDetail(
            category=self.category,
            message=str(self),
            owner=self.owner,
            retryable=self.retryable,
            meta=self.meta,
        )


class ConfigError(FederatedCoreError):
    category = FailureCategory.CONFIG
    owner = "caller"


class DataContractError(FederatedCoreError):
    category = FailureCategory.DATA_CONTRACT
    owner = "data_owner"


class PrivacyPolicyError(FederatedCoreError):
    category = FailureCategory.PRIVACY
    owner = "privacy_policy"


class InfrastructureError(FederatedCoreError):
    category = FailureCategory.INFRASTRUCTURE
    owner = "platform"
    retryable = True


class PartialFailureError(FederatedCoreError):
    category = FailureCategory.PARTIAL_FAILURE
    owner = "node_or_platform"
    retryable = True
