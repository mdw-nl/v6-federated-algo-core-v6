from dataclasses import dataclass
from typing import Optional, Tuple

from .types import MethodSpec


@dataclass(frozen=True)
class ReviewChecklistItem:
    key: str
    prompt: str
    required: bool = True
    completed: bool = False
    notes: Optional[str] = None


@dataclass(frozen=True)
class MethodReviewChecklist:
    items: Tuple[ReviewChecklistItem, ...]

    @classmethod
    def default(cls) -> "MethodReviewChecklist":
        return cls(
            items=(
                ReviewChecklistItem(
                    key="contracts",
                    prompt="Input and output schemas match the method contract.",
                ),
                ReviewChecklistItem(
                    key="payload-minimization",
                    prompt="Only minimum necessary fields cross node and central boundaries.",
                ),
                ReviewChecklistItem(
                    key="privacy",
                    prompt="Privacy thresholds and suppression rules were reviewed for this method.",
                ),
                ReviewChecklistItem(
                    key="failure-handling",
                    prompt="Failure paths return structured errors and do not silently degrade.",
                ),
                ReviewChecklistItem(
                    key="logging",
                    prompt="Logs help local debugging without leaking sensitive data.",
                ),
                ReviewChecklistItem(
                    key="tests",
                    prompt="Unit, MockAlgorithmClient, and infra tests were updated or confirmed.",
                ),
            )
        )

    def missing_required_items(self) -> Tuple[ReviewChecklistItem, ...]:
        return tuple(
            item for item in self.items if item.required and not item.completed
        )

    def is_complete(self) -> bool:
        return not self.missing_required_items()

    def assert_complete(self) -> None:
        missing = self.missing_required_items()
        if missing:
            missing_keys = ", ".join(item.key for item in missing)
            raise ValueError(
                f"Method review checklist is incomplete. Missing required items: {missing_keys}"
            )

    def mark(
        self,
        key: str,
        *,
        completed: bool = True,
        notes: Optional[str] = None,
    ) -> "MethodReviewChecklist":
        updated_items = []
        found = False

        for item in self.items:
            if item.key == key:
                found = True
                updated_items.append(
                    ReviewChecklistItem(
                        key=item.key,
                        prompt=item.prompt,
                        required=item.required,
                        completed=completed,
                        notes=notes if notes is not None else item.notes,
                    )
                )
            else:
                updated_items.append(item)

        if not found:
            raise KeyError(f"Unknown review checklist item: {key}")

        return MethodReviewChecklist(items=tuple(updated_items))


@dataclass(frozen=True)
class MethodReviewRecord:
    method_name: str
    checklist: MethodReviewChecklist

    @classmethod
    def for_method(
        cls,
        spec: MethodSpec,
        checklist: Optional[MethodReviewChecklist] = None,
    ) -> "MethodReviewRecord":
        return cls(
            method_name=spec.name,
            checklist=checklist or MethodReviewChecklist.default(),
        )

    def mark(
        self,
        key: str,
        *,
        completed: bool = True,
        notes: Optional[str] = None,
    ) -> "MethodReviewRecord":
        return MethodReviewRecord(
            method_name=self.method_name,
            checklist=self.checklist.mark(
                key,
                completed=completed,
                notes=notes,
            ),
        )

    def assert_complete(self) -> None:
        self.checklist.assert_complete()
