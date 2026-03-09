from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Type, TypeVar

from pydantic import BaseModel, ValidationError

from .errors import ConfigError, PartialFailureError
from .types import ResultEnvelope

StepInputModelT = TypeVar("StepInputModelT", bound=BaseModel)
StepOutputModelT = TypeVar("StepOutputModelT", bound=BaseModel)


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
        if isinstance(raw_result, ResultEnvelope):
            envelope = raw_result
        elif isinstance(raw_result, dict) and "ok" in raw_result:
            envelope = ResultEnvelope.model_validate(raw_result)
        else:
            envelope = None

        if envelope is not None:
            if not envelope.ok:
                raise PartialFailureError(
                    f"Workflow step '{step.name}' returned a failure envelope",
                    meta={
                        "step": step.name,
                        "method": step.dispatch_method,
                        "error_count": len(envelope.errors),
                    },
                )
            raw_result = envelope.payload or {}

        try:
            validated_output = step.validate_output(raw_result)
        except ValidationError:
            raise PartialFailureError(
                f"Workflow step '{step.name}' returned an invalid result payload",
                meta={
                    "step": step.name,
                    "method": step.dispatch_method,
                },
            ) from None

        return validated_output.model_dump()
