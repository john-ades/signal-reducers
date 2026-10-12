"""Release content guards must reject private data before installation."""

import io
import runpy
import tarfile
from pathlib import Path

import pytest


@pytest.fixture(scope="module")
def validator():
    return runpy.run_path(
        str(Path(__file__).resolve().parents[1] / "scripts/validate_release.py")
    )


@pytest.mark.parametrize(
    "name",
    [
        ".env",
        ".env.production",
        "outputs/summary.json",
        "models/model.safetensors",
        "private/legacy-docs/draft.md",
        "src/signal_reducers/__pycache__/models.pyc",
    ],
)
def test_release_rejects_private_or_generated_content(validator, name):
    with pytest.raises(ValueError):
        validator["check_content"](name, b"synthetic")


def test_placeholder_environment_is_publishable_but_credentials_are_not(validator):
    check = validator["check_content"]
    check(".env.example", b"OPENROUTER_API_KEY=\n")
    with pytest.raises(ValueError, match="credential_like"):
        check("README.md", b"sk-or-v1-" + b"A" * 24)
    with pytest.raises(ValueError, match="personal_absolute_path"):
        check("README.md", b"/Users/" + b"synthetic/Developer/project")


@pytest.mark.parametrize(
    "name", ["/src/unsafe.py", "project/../src/unsafe.py", "../src/unsafe.py"]
)
def test_source_archive_rejects_unsafe_paths_before_stripping_root(
    validator, tmp_path, name
):
    archive = tmp_path / "signal_reducers-0.1.0a1.tar.gz"
    with tarfile.open(archive, "w:gz") as output:
        member = tarfile.TarInfo(name)
        member.size = 1
        output.addfile(member, io.BytesIO(b"x"))
    with pytest.raises(ValueError, match="unsafe_archive_path"):
        validator["archive_members"](archive)
