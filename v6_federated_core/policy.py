from abc import ABC, abstractmethod
from enum import Enum
from typing import Any, Dict, Iterable, List, Optional

from pydantic import BaseModel, Field


class PolicyScope(str, Enum):
    ELIGIBILITY = "eligibility"
    EXECUTION = "execution"
    OUTPUT = "output"


class PolicyContext(BaseModel):
    scope: PolicyScope
    method: str
    organization_count: Optional[int] = None
    row_count: Optional[int] = None
    meta: Dict[str, Any] = Field(default_factory=dict)


class PolicyDecision(BaseModel):
    allowed: bool
    reason: Optional[str] = None
    warnings: List[str] = Field(default_factory=list)


class PrivacyPolicy(ABC):
    scope = PolicyScope.ELIGIBILITY
    name = "privacy-policy"

    @abstractmethod
    def evaluate(self, context: PolicyContext) -> PolicyDecision:
        raise NotImplementedError


class MinOrganizationsPolicy(PrivacyPolicy):
    name = "min-organizations"

    def __init__(self, minimum: int):
        self.minimum = minimum

    def evaluate(self, context: PolicyContext) -> PolicyDecision:
        count = context.organization_count
        if count is None:
            return PolicyDecision(
                allowed=False,
                reason="organization_count is required for min-organizations policy",
            )
        if count < self.minimum:
            return PolicyDecision(
                allowed=False,
                reason=(
                    f"Minimum organizations not met for '{context.method}': "
                    f"required={self.minimum}, actual={count}"
                ),
            )
        return PolicyDecision(allowed=True)


class MinRowsPolicy(PrivacyPolicy):
    name = "min-rows"

    def __init__(self, minimum: int):
        self.minimum = minimum

    def evaluate(self, context: PolicyContext) -> PolicyDecision:
        count = context.row_count
        if count is None:
            return PolicyDecision(
                allowed=False,
                reason="row_count is required for min-rows policy",
            )
        if count < self.minimum:
            return PolicyDecision(
                allowed=False,
                reason=(
                    f"Minimum rows not met for '{context.method}': "
                    f"required={self.minimum}, actual={count}"
                ),
            )
        return PolicyDecision(allowed=True)


def evaluate_policies(
    policies: Iterable[PrivacyPolicy],
    context: PolicyContext,
) -> PolicyDecision:
    warnings: List[str] = []

    for policy in policies:
        decision = policy.evaluate(context)
        warnings.extend(decision.warnings)
        if not decision.allowed:
            return PolicyDecision(
                allowed=False,
                reason=decision.reason,
                warnings=warnings,
            )

    return PolicyDecision(allowed=True, warnings=warnings)
