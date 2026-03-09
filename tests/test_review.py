import unittest

from pydantic import BaseModel

from v6_federated_core import MethodReviewRecord, MethodSpec


class DemoInput(BaseModel):
    value: int


class DemoOutput(BaseModel):
    doubled: int


def _double_handler(data: DemoInput, context=None):
    return {"doubled": data.value * 2}


class MethodReviewRecordTestCase(unittest.TestCase):
    def test_review_record_can_be_completed(self) -> None:
        spec = MethodSpec(
            name="double",
            input_model=DemoInput,
            output_model=DemoOutput,
            handler=_double_handler,
        )

        record = MethodReviewRecord.for_method(spec)
        self.assertFalse(record.checklist.is_complete())

        for key in [
            "contracts",
            "payload-minimization",
            "privacy",
            "failure-handling",
            "logging",
            "tests",
        ]:
            record = record.mark(key)

        record.assert_complete()
        self.assertTrue(record.checklist.is_complete())
