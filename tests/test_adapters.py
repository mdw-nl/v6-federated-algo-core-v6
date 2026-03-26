import unittest

from pydantic import BaseModel

from v6_federated_core import (
    ConfigError,
    MethodRegistry,
    MethodSpec,
    ResultEnvelope,
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

    def test_dispatch_registered_method_returns_success_payload(self) -> None:
        result = dispatch_registered_method(
            self.registry,
            "double",
            {"value": 3},
        )

        self.assertEqual(result, {"doubled": 6})

    def test_dispatch_task_input_validates_wrapper_shape(self) -> None:
        with self.assertRaises(ConfigError):
            dispatch_task_input(
                self.registry,
                {"kwargs": {"value": 3}},
            )

    def test_dispatch_task_input_validates_kwargs_type(self) -> None:
        with self.assertRaises(ConfigError):
            dispatch_task_input(
                self.registry,
                {"method": "double", "kwargs": "bad"},
            )

    def test_to_v6_result_passthrough_dict(self) -> None:
        payload = dispatch_task_input(
            self.registry,
            {"method": "double", "kwargs": {"value": 4}},
        )

        result = to_v6_result(payload)

        self.assertEqual(result, {"doubled": 8})

    def test_to_v6_result_raises_on_failure_envelope(self) -> None:
        envelope = ResultEnvelope(
            ok=False,
            errors=[
                {
                    "category": "config",
                    "message": "bad input",
                    "owner": "caller",
                }
            ],
        )
        with self.assertRaises(ConfigError):
            to_v6_result(envelope)
