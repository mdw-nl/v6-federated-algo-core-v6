from typing import Any, Dict, Iterable, Optional

from pydantic import ValidationError

from .errors import FederatedCoreError, PrivacyPolicyError
from .policy import PolicyContext, PrivacyPolicy, evaluate_policies
from .types import FailureCategory, FailureDetail, MethodContext, MethodSpec, ResultEnvelope


def _validation_failure(message: str, owner: str = "caller") -> FailureDetail:
    return FailureDetail(
        category=FailureCategory.CONFIG,
        message=message,
        owner=owner,
    )


def invoke_method(
    spec: MethodSpec,
    raw_input: Dict[str, Any],
    *,
    context: Optional[MethodContext] = None,
    policies: Optional[Iterable[PrivacyPolicy]] = None,
    policy_context: Optional[PolicyContext] = None,
) -> ResultEnvelope:
    method_context = context or MethodContext(method=spec.name)

    try:
        validated_input = spec.validate_input(raw_input)
    except ValidationError as exc:
        return ResultEnvelope.failure(
            _validation_failure(
                f"Input validation failed for '{spec.name}': {exc.__class__.__name__}",
            ),
            meta={"method": spec.name},
        )

    if policies:
        effective_policy_context = policy_context or PolicyContext(
            scope="eligibility",
            method=spec.name,
            organization_count=len(method_context.organization_ids),
        )
        decision = evaluate_policies(policies, effective_policy_context)
        if not decision.allowed:
            return ResultEnvelope.failure(
                PrivacyPolicyError(
                    decision.reason or f"Privacy policy blocked '{spec.name}'"
                ).to_failure_detail(),
                warnings=decision.warnings,
                meta={"method": spec.name},
            )

    try:
        raw_output = spec.handler(validated_input, method_context)
        validated_output = spec.validate_output(raw_output)
    except ValidationError as exc:
        return ResultEnvelope.failure(
            _validation_failure(
                f"Output validation failed for '{spec.name}': {exc.__class__.__name__}",
                owner="algorithm",
            ),
            meta={"method": spec.name},
        )
    except FederatedCoreError as exc:
        return ResultEnvelope.failure(
            exc.to_failure_detail(),
            meta={"method": spec.name},
        )
    except Exception:
        return ResultEnvelope.failure(
            FederatedCoreError(
                f"Unhandled execution failure in '{spec.name}'"
            ).to_failure_detail(),
            meta={"method": spec.name},
        )

    return ResultEnvelope.success(
        payload=validated_output.model_dump(),
        meta={"method": spec.name},
    )
