import unittest

from pydantic import BaseModel

from v6_federated_core import ConfigError, MethodRegistry, MethodSpec


class DemoInput(BaseModel):
    value: int


class DemoOutput(BaseModel):
    doubled: int


def _double_handler(data: DemoInput, context=None):
    return {"doubled": data.value * 2}


class MethodRegistryTestCase(unittest.TestCase):
    def test_register_and_get(self) -> None:
        spec = MethodSpec(
            name="double",
            input_model=DemoInput,
            output_model=DemoOutput,
            handler=_double_handler,
        )
        registry = MethodRegistry()

        registry.register(spec)

        self.assertEqual(registry.get("double"), spec)
        self.assertEqual(registry.names(), ["double"])

    def test_duplicate_registration_raises(self) -> None:
        spec = MethodSpec(
            name="double",
            input_model=DemoInput,
            output_model=DemoOutput,
            handler=_double_handler,
        )
        registry = MethodRegistry([spec])

        with self.assertRaises(ConfigError):
            registry.register(spec)

    def test_unknown_method_raises(self) -> None:
        registry = MethodRegistry()

        with self.assertRaises(ConfigError):
            registry.get("missing")
