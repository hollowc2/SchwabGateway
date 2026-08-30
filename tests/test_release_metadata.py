from __future__ import annotations

import tomllib
from pathlib import Path

import yaml


def _project_version(path: str) -> str:
    with Path(path).open("rb") as file:
        return tomllib.load(file)["project"]["version"]


def test_gateway_openapi_and_sdk_release_versions_match() -> None:
    gateway_version = _project_version("pyproject.toml")
    sdk_version = _project_version("packages/sdk/pyproject.toml")
    openapi_version = str(yaml.safe_load(Path("openapi.yaml").read_text())["info"]["version"])

    assert gateway_version == sdk_version == openapi_version


def test_wire_schema_version_remains_independent() -> None:
    contract = yaml.safe_load(Path("openapi.yaml").read_text())

    assert contract["components"]["schemas"]["SchemaVersion"]["const"] == "1.0"
