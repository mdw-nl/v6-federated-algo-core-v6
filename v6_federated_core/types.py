from dataclasses import dataclass
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Type, TypeVar

from pydantic import BaseModel, Field


class FailureCategory(str, Enum):
    CONFIG = "config"
    DATA_CONTRACT = "data_contract"
    PRIVACY = "privacy"
    EXECUTION = "execution"
    INFRASTRUCTURE = "infrastructure"
    PARTIAL_FAILURE = "partial_failure"


class FailureDetail(BaseModel):
    category: FailureCategory
    message: str
    owner: str
    retryable: bool = False
    node_id: Optional[int] = None
    meta: Dict[str, Any] = Field(default_factory=dict)


class ResultEnvelope(BaseModel):
    ok: bool
    payload: Optional[Dict[str, Any]] = None
    errors: List[FailureDetail] = Field(default_factory=list)
    warnings: List[str] = Field(default_factory=list)
    meta: Dict[str, Any] = Field(default_factory=dict)

    @classmethod
    def success(
        cls,
        payload: Dict[str, Any],
        warnings: Optional[List[str]] = None,
        meta: Optional[Dict[str, Any]] = None,
    ) -> "ResultEnvelope":
        return cls(
            ok=True,
            payload=payload,
            warnings=warnings or [],
            meta=meta or {},
        )

    @classmethod
    def failure(
        cls,
        error: FailureDetail,
        warnings: Optional[List[str]] = None,
        meta: Optional[Dict[str, Any]] = None,
    ) -> "ResultEnvelope":
        return cls(
            ok=False,
            errors=[error],
            warnings=warnings or [],
            meta=meta or {},
        )


class MethodContext(BaseModel):
    method: str
    organization_ids: List[int] = Field(default_factory=list)
    task_id: Optional[int] = None
    meta: Dict[str, Any] = Field(default_factory=dict)


InputModelT = TypeVar("InputModelT", bound=BaseModel)
OutputModelT = TypeVar("OutputModelT", bound=BaseModel)


@dataclass(frozen=True)
class MethodSpec:
    name: str
    input_model: Type[InputModelT]
    output_model: Type[OutputModelT]
    handler: Callable[[InputModelT, Optional[MethodContext]], Any]
    default_failure_category: FailureCategory = FailureCategory.EXECUTION

    def validate_input(self, raw_input: Dict[str, Any]) -> InputModelT:
        return self.input_model.model_validate(raw_input)

    def validate_output(self, raw_output: Any) -> OutputModelT:
        return self.output_model.model_validate(raw_output)
