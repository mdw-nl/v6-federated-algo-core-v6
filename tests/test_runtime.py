import unittest

from pydantic import BaseModel

from v6_federated_core import (
    ConfigError,
    DataContractError,
    MethodContext,
    MethodSpec,
    MinOrganizationsPolicy,
    PolicyContext,
    PolicyScope,
    PrivacyPolicyError,
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

        self.assertEqual(result, {"doubled": 10})

    def test_invoke_method_raises_on_input_validation(self) -> None:
        spec = MethodSpec(
            name="double",
            input_model=DemoInput,
            output_model=DemoOutput,
            handler=_double_handler,
        )

        with self.assertRaises(ConfigError):
            invoke_method(spec, {"value": "oops"})

    def test_invoke_method_raises_on_output_validation(self) -> None:
        spec = MethodSpec(
            name="double",
            input_model=DemoInput,
            output_model=DemoOutput,
            handler=_bad_output_handler,
        )

        with self.assertRaises(DataContractError):
            invoke_method(spec, {"value": 5})

    def test_invoke_method_raises_on_privacy_policy(self) -> None:
        spec = MethodSpec(
            name="double",
            input_model=DemoInput,
            output_model=DemoOutput,
            handler=_double_handler,
        )
        context = MethodContext(method="double", organization_ids=[1, 2])

        with self.assertRaises(PrivacyPolicyError):
            invoke_method(
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

    def test_invoke_method_preserves_unhandled_error(self) -> None:
        spec = MethodSpec(
            name="double",
            input_model=DemoInput,
            output_model=DemoOutput,
            handler=_crash_handler,
        )

        with self.assertRaises(RuntimeError) as exc_info:
            invoke_method(spec, {"value": 5})

        self.assertEqual(str(exc_info.exception), "boom")

    def test_invoke_method_collects_error_details_in_context(self) -> None:
        spec = MethodSpec(
            name="double",
            input_model=DemoInput,
            output_model=DemoOutput,
            handler=_crash_handler,
        )
        context = MethodContext(method="double")

        with self.assertRaises(RuntimeError):
            invoke_method(spec, {"value": 5}, context=context)

        collected = context.meta.get("errors", [])
        self.assertTrue(collected)
        self.assertEqual(collected[0]["category"], "execution")
