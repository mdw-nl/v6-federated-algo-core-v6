import unittest

from pydantic import BaseModel

from v6_federated_core import (
    ConfigError,
    DataContractError,
    FederatedCoreError,
    InfrastructureError,
    PartialFailureError,
    ResultEnvelope,
    TaskRunner,
    WorkflowStepSpec,
    parse_result_envelope,
)


class StepInput(BaseModel):
    value: int


class StepOutput(BaseModel):
    doubled: int


class _TaskAPI:
    def __init__(self, client):
        self._client = client

    def create(self, **kwargs):
        self._client.created.append(kwargs)
        return {"id": 1}


class FakeClient:
    def __init__(self, results):
        self.results = results
        self.created = []
        self.task = _TaskAPI(self)

    def wait_for_results(self, task_id, interval):
        self.last_wait = {"task_id": task_id, "interval": interval}
        return self.results


class TaskRunnerTestCase(unittest.TestCase):
    def setUp(self) -> None:
        self.step = WorkflowStepSpec(
            name="double-step",
            namespace="linear",
            method="partial",
            input_model=StepInput,
            output_model=StepOutput,
        )

    def test_create_task_input_uses_namespaced_method(self) -> None:
        runner = TaskRunner(FakeClient(results=[]))

        task_input = runner.create_task_input(self.step, {"value": 4})

        self.assertEqual(
            task_input,
            {"method": "linear.partial", "kwargs": {"value": 4}},
        )

    def test_create_task_input_uses_dispatch_override(self) -> None:
        runner = TaskRunner(FakeClient(results=[]))
        step = WorkflowStepSpec(
            name="double-step",
            namespace="linear",
            method="partial",
            task_method="linear_partial",
            input_model=StepInput,
            output_model=StepOutput,
        )

        task_input = runner.create_task_input(step, {"value": 4})

        self.assertEqual(
            task_input,
            {"method": "linear_partial", "kwargs": {"value": 4}},
        )

    def test_run_dispatches_and_collects_validated_results(self) -> None:
        client = FakeClient(results=[{"doubled": 8}, {"doubled": 10}])
        runner = TaskRunner(client, default_interval=3)

        results = runner.run(self.step, {"value": 4}, [1, 2])

        self.assertEqual(results, [{"doubled": 8}, {"doubled": 10}])
        self.assertEqual(
            client.created[0],
            {
                "input_": {"method": "linear.partial", "kwargs": {"value": 4}},
                "organizations": [1, 2],
            },
        )
        self.assertEqual(client.last_wait, {"task_id": 1, "interval": 3})

    def test_collect_unwraps_success_envelope(self) -> None:
        client = FakeClient(results=[ResultEnvelope.success(payload={"doubled": 12})])
        runner = TaskRunner(client)

        results = runner.collect(self.step, 1)

        self.assertEqual(results, [{"doubled": 12}])

    def test_collect_raises_on_failure_envelope(self) -> None:
        client = FakeClient(
            results=[
                ResultEnvelope.failure(
                    PartialFailureError("node failed").to_failure_detail()
                ).model_dump()
            ]
        )
        runner = TaskRunner(client)

        with self.assertRaises(PartialFailureError):
            runner.collect(self.step, 1)

    def test_collect_raises_on_malformed_failure_envelope(self) -> None:
        client = FakeClient(
            results=[
                {
                    "ok": False,
                    "errors": [{"message": "node failed without category"}],
                    "meta": {"method": "linear.partial"},
                }
            ]
        )
        runner = TaskRunner(client)

        with self.assertRaises(FederatedCoreError) as exc_info:
            runner.collect(self.step, 1)

        self.assertEqual(str(exc_info.exception), "node failed without category")
        self.assertEqual(exc_info.exception.meta.get("errors", [{}])[0].get("category"), "execution")

    def test_dispatch_rejects_empty_organizations(self) -> None:
        runner = TaskRunner(FakeClient(results=[]))

        with self.assertRaises(ConfigError):
            runner.dispatch(self.step, {"value": 4}, [])

    def test_dispatch_rejects_reserved_task_options(self) -> None:
        runner = TaskRunner(FakeClient(results=[]))

        with self.assertRaises(ConfigError):
            runner.dispatch(
                self.step,
                {"value": 4},
                [1],
                task_options={"input_": {"method": "bad"}},
            )

    def test_collect_raises_on_invalid_payload(self) -> None:
        client = FakeClient(results=[{"wrong": 1}])
        runner = TaskRunner(client)

        with self.assertRaises(DataContractError):
            runner.collect(self.step, 1)

    def test_collect_raises_on_non_list_payload(self) -> None:
        client = FakeClient(results={"wrong": 1})
        runner = TaskRunner(client)

        with self.assertRaises(InfrastructureError):
            runner.collect(self.step, 1)

    def test_parse_result_envelope_coerces_schema_drift(self) -> None:
        envelope = parse_result_envelope(
            {
                "ok": False,
                "errors": [{"message": "upstream failure"}],
            }
        )

        self.assertIsNotNone(envelope)
        assert envelope is not None
        self.assertFalse(envelope.ok)
        self.assertEqual(envelope.errors[0].category.value, "execution")
        self.assertEqual(envelope.errors[0].message, "upstream failure")
