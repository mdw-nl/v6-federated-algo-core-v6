from typing import Any, Dict, Iterable, Optional

from .errors import ConfigError, FederatedCoreError
from .orchestration import error_from_envelope
from .policy import PolicyContext, PrivacyPolicy
from .registry import MethodRegistry
from .runtime import invoke_method
from .types import MethodContext, ResultEnvelope


def dispatch_registered_method(
    registry: MethodRegistry,
    method_name: str,
    raw_input: Dict[str, Any],
    *,
    context: Optional[MethodContext] = None,
    policies: Optional[Iterable[PrivacyPolicy]] = None,
    policy_context: Optional[PolicyContext] = None,
) -> Dict[str, Any]:
    spec = registry.get(method_name)
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
) -> Dict[str, Any]:
    if not isinstance(task_input, dict):
        raise ConfigError("Task input must be a dictionary")

    method_name = task_input.get("method")
    if not isinstance(method_name, str) or not method_name.strip():
        raise ConfigError("Task input must include a non-empty 'method'")

    raw_kwargs = task_input.get("kwargs", {})
    if raw_kwargs is None:
        raw_kwargs = {}
    if not isinstance(raw_kwargs, dict):
        raise ConfigError(
            "Task input 'kwargs' must be a dictionary",
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
    result: Any,
    *,
    include_meta_on_success: bool = False,
) -> Dict[str, Any]:
    if isinstance(result, dict):
        return result

    if isinstance(result, ResultEnvelope):
        if result.ok:
            payload = result.payload or {}
            if include_meta_on_success:
                return result.model_dump()
            return payload
        raise error_from_envelope(result)

    if hasattr(result, "model_dump"):
        dumped = result.model_dump()
        if isinstance(dumped, dict):
            return dumped

    raise FederatedCoreError(
        "Method result must be a dictionary-like payload",
        meta={"result_type": type(result).__name__},
    )
