from signal_reducers.cli import workflow_qualification


def test_nine_successful_workflows_cannot_hide_a_lifecycle_violation():
    reports = [{"passed": True, "lifecycle_violations": 0} for _ in range(9)]
    reports.append({"passed": False, "lifecycle_violations": 1})
    result = workflow_qualification(reports, 10, "live")
    assert result["successful_runs"] == 9
    assert not result["passed"]
    assert not result["zero_lifecycle_violations"]


def test_workflow_success_requires_all_runs_and_separate_router_qualification():
    reports = [{"passed": True, "lifecycle_violations": 0} for _ in range(9)]
    assert not workflow_qualification(reports, 10, "live")["passed"]
    reports.append({"passed": False, "lifecycle_violations": 0})
    result = workflow_qualification(reports, 10, "live")
    assert result["passed"]
    assert result["qualification_scope"] == "workflows_only"
    assert "held-out" in result["router_qualification"]


async def test_oversized_publication_rejects_before_validating_records(tmp_path):
    import json
    from argparse import Namespace
    from uuid import uuid4

    import pytest

    from signal_reducers.cli import execute
    from signal_reducers.models import ContractError

    path = tmp_path / "signals.json"
    path.write_text(json.dumps([{}] * 101))
    with pytest.raises(ContractError) as error:
        await execute(
            Namespace(
                config=None,
                redis_url=None,
                command="publish",
                file=path,
                run_id=uuid4(),
            )
        )
    assert error.value.code == "publisher_batch_capacity"


async def test_present_invalid_key_fails_readiness_before_catalog_cpu_or_inference(
    monkeypatch,
):
    import httpx

    import signal_reducers.cli as cli
    from signal_reducers.config import Config
    from signal_reducers.providers import OpenRouterClient

    paths, clients = [], []

    class Queue:
        def __init__(self, *_):
            pass

        async def check(self):
            return {"ready": True}

        async def close(self):
            pass

    class Classifier:
        def __init__(self, *_):
            pass

        async def prepare(self):
            raise AssertionError("CPU should not prepare with invalid credentials")

    def handler(req):
        paths.append(req.url.path)
        assert req.url.path == "/api/v1/key"
        return httpx.Response(
            401, json={"error": {"message": "private provider details"}}
        )

    def factory(config):
        client = OpenRouterClient(config, "invalid", httpx.MockTransport(handler))
        clients.append(client)
        return client

    monkeypatch.setenv("OPENROUTER_API_KEY", "invalid")
    monkeypatch.setattr(cli, "SignalQueue", Queue)
    monkeypatch.setattr(cli, "LayaClassifier", Classifier)
    monkeypatch.setattr(cli, "OpenRouterClient", factory)
    result = await cli.readiness(Config(mode="live"), probe=True)
    assert result["openrouter_key_present"] is True
    assert result["openrouter_authenticated"] is False
    assert (
        result["ready"] is False and result["model_error"] == "provider_authentication"
    )
    assert "models" not in result and "probes_passed" not in result
    assert paths == ["/api/v1/key"] and clients[0]._http.is_closed


async def test_live_readiness_authenticates_before_public_catalog(monkeypatch):
    import httpx

    import signal_reducers.cli as cli
    from signal_reducers.config import Config
    from signal_reducers.providers import OpenRouterClient

    paths = []

    class Queue:
        def __init__(self, *_):
            pass

        async def check(self):
            return {"ready": True}

        async def close(self):
            pass

    def handler(req):
        paths.append(req.url.path)
        if req.url.path == "/api/v1/key":
            return httpx.Response(200, json={"data": {"label": "private metadata"}})
        assert req.url.path == "/api/v1/models"
        return httpx.Response(
            200,
            json={
                "data": [
                    {
                        "id": "nace-ai/drex-v1.5",
                        "architecture": {"output_modalities": ["decisions"]},
                    },
                    {
                        "id": "openai/gpt-4.1-mini",
                        "supported_parameters": ["response_format"],
                    },
                    {
                        "id": "openai/gpt-5.1",
                        "supported_parameters": ["response_format", "reasoning"],
                        "reasoning": {"supported_efforts": ["none", "low", "high"]},
                    },
                ]
            },
        )

    monkeypatch.setenv("OPENROUTER_API_KEY", "valid")
    monkeypatch.setattr(cli, "SignalQueue", Queue)
    monkeypatch.setattr(
        cli,
        "OpenRouterClient",
        lambda config: OpenRouterClient(config, "valid", httpx.MockTransport(handler)),
    )
    result = await cli.readiness(Config(mode="live"))
    assert result["ready"] is True and result["openrouter_authenticated"] is True
    assert result["inference_qualified"] is False
    assert paths == ["/api/v1/key", "/api/v1/models"]


def test_version_uses_installed_project_metadata(capsys):
    from importlib.metadata import version

    import pytest

    from signal_reducers.cli import parser

    with pytest.raises(SystemExit) as outcome:
        parser().parse_args(["--version"])
    assert outcome.value.code == 0
    assert capsys.readouterr().out == f"signal-reducers {version('signal-reducers')}\n"
