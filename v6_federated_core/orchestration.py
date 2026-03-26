from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Type, TypeVar

from pydantic import BaseModel, ValidationError

from .errors import (
    ConfigError,
    DataContractError,
    FederatedCoreError,
    InfrastructureError,
    PartialFailureError,
    PrivacyPolicyError,
)
from .types import FailureCategory, FailureDetail, ResultEnvelope

StepInputModelT = TypeVar("StepInputModelT", bound=BaseModel)
StepOutputModelT = TypeVar("StepOutputModelT", bound=BaseModel)


def _normalize_warnings(raw_warnings: Any) -> List[str]:
    if raw_warnings is None:
        return []
    if isinstance(raw_warnings, list):
        return [str(item) for item in raw_warnings]
    return [str(raw_warnings)]


def _normalize_meta(raw_meta: Any) -> Dict[str, Any]:
    if raw_meta is None:
        return {}
    if isinstance(raw_meta, dict):
        return dict(raw_meta)
    return {"raw_meta": raw_meta}


def _coerce_failure_category(raw_category: Any) -> FailureCategory:
    if isinstance(raw_category, FailureCategory):
        return raw_category
    if isinstance(raw_category, str):
        try:
            return FailureCategory(raw_category)
        except ValueError:
            pass
    return FailureCategory.EXECUTION


def _coerce_failure_detail(raw_error: Any) -> FailureDetail:
    if isinstance(raw_error, FailureDetail):
        return raw_error

    if isinstance(raw_error, dict):
        message = raw_error.get("message")
        if not isinstance(message, str) or not message.strip():
            message = "Task returned a malformed failure detail"

        owner = raw_error.get("owner")
        if not isinstance(owner, str) or not owner.strip():
            owner = "node_or_platform"

        node_id = raw_error.get("node_id")
        if not isinstance(node_id, int):
            node_id = None

        meta = _normalize_meta(raw_error.get("meta"))
        extra_fields = {
            k: v
            for k, v in raw_error.items()
            if k not in {"category", "message", "owner", "retryable", "node_id", "meta"}
        }
        if extra_fields:
            meta["raw_fields"] = extra_fields

        return FailureDetail(
            category=_coerce_failure_category(raw_error.get("category")),
            message=message,
            owner=owner,
            retryable=bool(raw_error.get("retryable", False)),
            node_id=node_id,
            meta=meta,
        )

    if isinstance(raw_error, str):
        message = raw_error
    else:
        message = "Task returned a non-dictionary failure detail"

    return FailureDetail(
        category=FailureCategory.EXECUTION,
        message=message,
        owner="node_or_platform",
        meta={"raw_error": repr(raw_error)},
    )


def parse_result_envelope(raw_result: Any) -> Optional[ResultEnvelope]:
    """Parse a task result into a ResultEnvelope when possible.

    This parser is tolerant to minor schema drift and malformed error items so
    callers can fail with a structured partial-failure instead of crashing on
    strict validation.
    """
    if isinstance(raw_result, ResultEnvelope):
        return raw_result
    if not isinstance(raw_result, dict) or "ok" not in raw_result:
        return None

    try:
        return ResultEnvelope.model_validate(raw_result)
    except ValidationError:
        ok = bool(raw_result.get("ok"))
        warnings = _normalize_warnings(raw_result.get("warnings"))
        meta = _normalize_meta(raw_result.get("meta"))
        meta.setdefault("raw_envelope", raw_result)

        if ok:
            payload = raw_result.get("payload")
            if not isinstance(payload, dict):
                payload = {}
            return ResultEnvelope(
                ok=True,
                payload=payload,
                warnings=warnings,
                meta=meta,
            )

        raw_errors = raw_result.get("errors")
        if isinstance(raw_errors, list):
            errors = [_coerce_failure_detail(item) for item in raw_errors]
        elif raw_errors is None:
            errors = []
        else:
            errors = [_coerce_failure_detail(raw_errors)]

        if not errors:
            fallback_message = raw_result.get("message")
            if not isinstance(fallback_message, str) or not fallback_message.strip():
                fallback_message = "Task returned failure envelope without structured errors"
            errors = [
                FailureDetail(
                    category=FailureCategory.EXECUTION,
                    message=fallback_message,
                    owner="node_or_platform",
                    meta={"raw_envelope": raw_result},
                )
            ]

        return ResultEnvelope(
            ok=False,
            errors=errors,
            warnings=warnings,
            meta=meta,
        )


def _exception_from_failure_detail(detail: FailureDetail) -> FederatedCoreError:
    error_meta = {
        "owner": detail.owner,
        "retryable": detail.retryable,
        "node_id": detail.node_id,
        **detail.meta,
    }
    category = detail.category
    if category == FailureCategory.CONFIG:
        return ConfigError(detail.message, meta=error_meta)
    if category == FailureCategory.DATA_CONTRACT:
        return DataContractError(detail.message, meta=error_meta)
    if category == FailureCategory.PRIVACY:
        return PrivacyPolicyError(detail.message, meta=error_meta)
    if category == FailureCategory.INFRASTRUCTURE:
        return InfrastructureError(detail.message, meta=error_meta)
    if category == FailureCategory.PARTIAL_FAILURE:
        return PartialFailureError(detail.message, meta=error_meta)
    return FederatedCoreError(detail.message, meta=error_meta)


def error_from_envelope(
    envelope: ResultEnvelope,
    *,
    default_message: str = "Task returned a failure envelope",
) -> FederatedCoreError:
    if envelope.errors:
        error = _exception_from_failure_detail(envelope.errors[0])
        error.meta.setdefault("errors", [detail.model_dump() for detail in envelope.errors])
    else:
        error = PartialFailureError(
            default_message,
            meta={"errors": []},
        )
    error.meta.setdefault("warnings", list(envelope.warnings))
    error.meta.setdefault("envelope_meta", dict(envelope.meta))
    return error


@dataclass(frozen=True)
class WorkflowStepSpec:
    name: str
    method: str
    input_model: Type[StepInputModelT]
    output_model: Type[StepOutputModelT]
    namespace: str = ""
    task_method: Optional[str] = None

    @property
    def qualified_method(self) -> str:
        if not self.namespace:
            return self.method
        return f"{self.namespace}.{self.method}"

    @property
    def dispatch_method(self) -> str:
        return self.task_method or self.qualified_method

    def validate_input(self, raw_input: Dict[str, Any]) -> StepInputModelT:
        return self.input_model.model_validate(raw_input)

    def validate_output(self, raw_output: Any) -> StepOutputModelT:
        return self.output_model.model_validate(raw_output)


class TaskRunner:
    def __init__(self, client: Any, *, default_interval: int = 1):
        self.client = client
        self.default_interval = default_interval

    def create_task_input(
        self,
        step: WorkflowStepSpec,
        raw_input: Dict[str, Any],
    ) -> Dict[str, Any]:
        validated_input = step.validate_input(raw_input)
        return {
            "method": step.dispatch_method,
            "kwargs": validated_input.model_dump(),
        }

    def dispatch(
        self,
        step: WorkflowStepSpec,
        raw_input: Dict[str, Any],
        organizations: List[int],
        *,
        task_options: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        if not organizations:
            raise ConfigError(
                f"Workflow step '{step.name}' requires at least one organization"
            )

        options = dict(task_options or {})
        reserved_keys = {"input_", "organizations"}
        if reserved_keys & options.keys():
            raise ConfigError(
                "task_options cannot override 'input_' or 'organizations'"
            )

        return self.client.task.create(
            input_=self.create_task_input(step, raw_input),
            organizations=organizations,
            **options,
        )

    def collect(
        self,
        step: WorkflowStepSpec,
        task_id: int,
        *,
        interval: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        results = self.client.wait_for_results(
            task_id=task_id,
            interval=interval or self.default_interval,
        )
        if not isinstance(results, list):
            raise InfrastructureError(
                "Task results payload is not a list",
                meta={
                    "step": step.name,
                    "method": step.dispatch_method,
                    "results_type": type(results).__name__,
                },
            )
        if not results:
            raise PartialFailureError(
                f"Workflow step '{step.name}' returned no results",
                meta={
                    "step": step.name,
                    "method": step.dispatch_method,
                    "task_id": task_id,
                },
            )
        return [self._normalize_result(step, result) for result in results]

    def run(
        self,
        step: WorkflowStepSpec,
        raw_input: Dict[str, Any],
        organizations: List[int],
        *,
        interval: Optional[int] = None,
        task_options: Optional[Dict[str, Any]] = None,
    ) -> List[Dict[str, Any]]:
        task = self.dispatch(
            step,
            raw_input,
            organizations,
            task_options=task_options,
        )
        return self.collect(
            step,
            task["id"],
            interval=interval,
        )

    def _normalize_result(
        self,
        step: WorkflowStepSpec,
        raw_result: Any,
    ) -> Dict[str, Any]:
        envelope = parse_result_envelope(raw_result)

        if envelope is not None:
            if not envelope.ok:
                error = error_from_envelope(
                    envelope,
                    default_message=f"Workflow step '{step.name}' returned a failure envelope",
                )
                error.meta.setdefault("step", step.name)
                error.meta.setdefault("method", step.dispatch_method)
                raise error
            raw_result = envelope.payload or {}

        try:
            validated_output = step.validate_output(raw_result)
        except ValidationError as exc:
            raise DataContractError(
                f"Workflow step '{step.name}' returned an invalid result payload",
                meta={
                    "step": step.name,
                    "method": step.dispatch_method,
                    "validation_errors": exc.errors(),
                },
            ) from exc

        return validated_output.model_dump()
