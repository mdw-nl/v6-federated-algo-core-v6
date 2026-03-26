import unittest

from pydantic import BaseModel

from v6_federated_core import (
    MethodContext,
    MethodSpec,
    MinOrganizationsPolicy,
    PolicyContext,
    PolicyScope,
    invoke_method,
)


class DemoInput(BaseModel):
    value: int


class DemoOutput(BaseModel):
    doubled: int


def _double_handler(data: DemoInput, context=None):
    return {"doubled": data.value * 2}


def _bad_output_handler(data: DemoInput, context=None):
    return {"wrong": data.value}


def _crash_handler(data: DemoInput, context=None):
    raise RuntimeError("boom")


class RuntimeTestCase(unittest.TestCase):
    def test_invoke_method_validates_and_executes(self) -> None:
        spec = MethodSpec(
            name="double",
            input_model=DemoInput,
            output_model=DemoOutput,
            handler=_double_handler,
        )

        result = invoke_method(spec, {"value": 5})

        self.assertTrue(result.ok)
        self.assertEqual(result.payload, {"doubled": 10})

    def test_invoke_method_returns_failure_on_output_validation(self) -> None:
        spec = MethodSpec(
            name="double",
            input_model=DemoInput,
            output_model=DemoOutput,
            handler=_bad_output_handler,
        )

        result = invoke_method(spec, {"value": 5})

        self.assertFalse(result.ok)
        self.assertEqual(result.errors[0].category.value, "config")

    def test_invoke_method_applies_privacy_policy(self) -> None:
        spec = MethodSpec(
            name="double",
            input_model=DemoInput,
            output_model=DemoOutput,
            handler=_double_handler,
        )
        context = MethodContext(method="double", organization_ids=[1, 2])

        result = invoke_method(
            spec,
            {"value": 5},
            context=context,
            policies=[MinOrganizationsPolicy(minimum=3)],
            policy_context=PolicyContext(
                scope=PolicyScope.ELIGIBILITY,
                method="double",
                organization_count=2,
            ),
        )

        self.assertFalse(result.ok)
        self.assertEqual(result.errors[0].category.value, "privacy")

    def test_invoke_method_includes_traceback_on_unhandled_error(self) -> None:
        spec = MethodSpec(
            name="double",
            input_model=DemoInput,
            output_model=DemoOutput,
            handler=_crash_handler,
        )

        result = invoke_method(spec, {"value": 5})

        self.assertFalse(result.ok)
        self.assertEqual(result.errors[0].category.value, "execution")
        self.assertEqual(
            result.errors[0].message,
            "Unhandled execution failure in 'double'",
        )
        self.assertEqual(result.errors[0].meta.get("exception_type"), "RuntimeError")
        self.assertEqual(result.errors[0].meta.get("exception_message"), "boom")
        self.assertIn(
            "Traceback (most recent call last)",
            result.errors[0].meta.get("traceback", ""),
        )
