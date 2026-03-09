import unittest

from pydantic import BaseModel

from v6_federated_core import (
    MethodRegistry,
    MethodSpec,
    dispatch_registered_method,
    dispatch_task_input,
    to_v6_result,
)


class DemoInput(BaseModel):
    value: int


class DemoOutput(BaseModel):
    doubled: int


def _double_handler(data: DemoInput, context=None):
    return {"doubled": data.value * 2}


class AdaptersTestCase(unittest.TestCase):
    def setUp(self) -> None:
        self.spec = MethodSpec(
            name="double",
            input_model=DemoInput,
            output_model=DemoOutput,
            handler=_double_handler,
        )
        self.registry = MethodRegistry([self.spec])

    def test_dispatch_registered_method_returns_success_envelope(self) -> None:
        result = dispatch_registered_method(
            self.registry,
            "double",
            {"value": 3},
        )

        self.assertTrue(result.ok)
        self.assertEqual(result.payload, {"doubled": 6})

    def test_dispatch_task_input_validates_wrapper_shape(self) -> None:
        result = dispatch_task_input(
            self.registry,
            {"kwargs": {"value": 3}},
        )

        self.assertFalse(result.ok)
        self.assertEqual(result.errors[0].category.value, "config")

    def test_dispatch_task_input_validates_kwargs_type(self) -> None:
        result = dispatch_task_input(
            self.registry,
            {"method": "double", "kwargs": "bad"},
        )

        self.assertFalse(result.ok)
        self.assertEqual(result.meta["method"], "double")

    def test_to_v6_result_unwraps_success_payload(self) -> None:
        envelope = dispatch_task_input(
            self.registry,
            {"method": "double", "kwargs": {"value": 4}},
        )

        result = to_v6_result(envelope)

        self.assertEqual(result, {"doubled": 8})

    def test_to_v6_result_keeps_failure_envelope(self) -> None:
        envelope = dispatch_task_input(
            self.registry,
            {"method": "missing", "kwargs": {}},
        )

        result = to_v6_result(envelope)

        self.assertFalse(result["ok"])
        self.assertEqual(result["errors"][0]["category"], "config")
