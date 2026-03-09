from typing import Any, Dict, Iterable, Optional

from .errors import ConfigError
from .policy import PolicyContext, PrivacyPolicy
from .registry import MethodRegistry
from .runtime import invoke_method
from .types import FailureCategory, FailureDetail, MethodContext, ResultEnvelope


def _config_failure(
    message: str,
    *,
    method_name: Optional[str] = None,
) -> ResultEnvelope:
    meta = {"method": method_name} if method_name else {}
    return ResultEnvelope.failure(
        FailureDetail(
            category=FailureCategory.CONFIG,
            message=message,
            owner="caller",
        ),
        meta=meta,
    )


def dispatch_registered_method(
    registry: MethodRegistry,
    method_name: str,
    raw_input: Dict[str, Any],
    *,
    context: Optional[MethodContext] = None,
    policies: Optional[Iterable[PrivacyPolicy]] = None,
    policy_context: Optional[PolicyContext] = None,
) -> ResultEnvelope:
    try:
        spec = registry.get(method_name)
    except ConfigError as exc:
        return ResultEnvelope.failure(
            exc.to_failure_detail(),
            meta={"method": method_name},
        )

    effective_context = context or MethodContext(method=method_name)
    return invoke_method(
        spec,
        raw_input,
        context=effective_context,
        policies=policies,
        policy_context=policy_context,
    )


def dispatch_task_input(
    registry: MethodRegistry,
    task_input: Dict[str, Any],
    *,
    context: Optional[MethodContext] = None,
    policies: Optional[Iterable[PrivacyPolicy]] = None,
    policy_context: Optional[PolicyContext] = None,
) -> ResultEnvelope:
    if not isinstance(task_input, dict):
        return _config_failure("Task input must be a dictionary")

    method_name = task_input.get("method")
    if not isinstance(method_name, str) or not method_name.strip():
        return _config_failure("Task input must include a non-empty 'method'")

    raw_kwargs = task_input.get("kwargs", {})
    if raw_kwargs is None:
        raw_kwargs = {}
    if not isinstance(raw_kwargs, dict):
        return _config_failure(
            "Task input 'kwargs' must be a dictionary",
            method_name=method_name,
        )

    return dispatch_registered_method(
        registry,
        method_name,
        raw_kwargs,
        context=context,
        policies=policies,
        policy_context=policy_context,
    )


def to_v6_result(
    envelope: ResultEnvelope,
    *,
    include_meta_on_success: bool = False,
) -> Dict[str, Any]:
    if envelope.ok and not include_meta_on_success:
        return envelope.payload or {}
    return envelope.model_dump()
