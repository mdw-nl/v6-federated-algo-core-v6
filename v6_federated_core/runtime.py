from typing import Any, Dict, Iterable, Optional

from pydantic import ValidationError

from .errors import ConfigError, DataContractError, FederatedCoreError, PrivacyPolicyError
from .policy import PolicyContext, PrivacyPolicy, evaluate_policies
from .types import FailureDetail, MethodContext, MethodSpec


def invoke_method(
    spec: MethodSpec,
    raw_input: Dict[str, Any],
    *,
    context: Optional[MethodContext] = None,
    policies: Optional[Iterable[PrivacyPolicy]] = None,
    policy_context: Optional[PolicyContext] = None,
) -> Dict[str, Any]:
    method_context = context or MethodContext(method=spec.name)

    def _record_error(detail: FailureDetail) -> None:
        errors = method_context.meta.setdefault("errors", [])
        if isinstance(errors, list):
            errors.append(detail.model_dump())

    try:
        validated_input = spec.validate_input(raw_input)
    except ValidationError as exc:
        error = ConfigError(
            f"Input validation failed for '{spec.name}'",
            meta={
                "method": spec.name,
                "validation_errors": exc.errors(),
            },
        )
        _record_error(error.to_failure_detail())
        raise error from exc

    if policies:
        effective_policy_context = policy_context or PolicyContext(
            scope="eligibility",
            method=spec.name,
            organization_count=len(method_context.organization_ids),
        )
        decision = evaluate_policies(policies, effective_policy_context)
        if not decision.allowed:
            error = PrivacyPolicyError(
                decision.reason or f"Privacy policy blocked '{spec.name}'",
                meta={
                    "method": spec.name,
                    "warnings": decision.warnings,
                },
            )
            _record_error(error.to_failure_detail())
            raise error

    try:
        raw_output = spec.handler(validated_input, method_context)
    except ValidationError as exc:
        # Keep the original validation traceback from the handler.
        _record_error(
            FederatedCoreError(
                f"Unhandled execution failure in '{spec.name}'",
                meta={
                    "method": spec.name,
                    "exception_type": exc.__class__.__name__,
                    "exception_message": str(exc),
                },
            ).to_failure_detail()
        )
        raise
    except FederatedCoreError as exc:
        _record_error(exc.to_failure_detail())
        raise
    except Exception as exc:
        _record_error(
            FederatedCoreError(
                f"Unhandled execution failure in '{spec.name}'",
                meta={
                    "method": spec.name,
                    "exception_type": exc.__class__.__name__,
                    "exception_message": str(exc),
                },
            ).to_failure_detail()
        )
        raise

    try:
        validated_output = spec.validate_output(raw_output)
    except ValidationError as exc:
        error = DataContractError(
            f"Output validation failed for '{spec.name}'",
            meta={
                "method": spec.name,
                "validation_errors": exc.errors(),
            },
        )
        _record_error(error.to_failure_detail())
        raise error from exc

    return validated_output.model_dump()
