"""Check release archives and isolated installs without models or hosted calls."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import shutil
import subprocess
import sys
import sysconfig
import tarfile
import tempfile
import venv
import zipfile
from collections import Counter
from pathlib import Path, PurePosixPath

DATASET_SHA256 = "041ec6daef323b8cb69df13758b5eadd694d5fc82e47d383b383c924114d33e0"
ROOT_FILES = {
    "README.md",
    "RUNNING.md",
    "LICENSE",
    "THIRD_PARTY_NOTICES.md",
    "CONTRIBUTING.md",
    "SECURITY.md",
    "CHANGELOG.md",
    "pyproject.toml",
    "uv.lock",
    "config.example.toml",
    ".python-version",
    ".gitignore",
    ".env.example",
    "PKG-INFO",
}
ROOT_DIRECTORIES = {"src", "scripts", "tests", "docs", ".github"}
EXCLUDED = {
    ".git",
    ".venv",
    ".idea",
    "__pycache__",
    ".pytest_cache",
    ".ruff_cache",
    "private",
    "models",
    "outputs",
    "build",
    "dist",
    ".DS_Store",
}


def check_content(name: str, data: bytes) -> None:
    parts = PurePosixPath(name).parts
    if any(part in EXCLUDED for part in parts):
        raise ValueError(f"private_or_generated_archive_member:{name}")
    if any(part.startswith(".env") and part != ".env.example" for part in parts):
        raise ValueError(f"environment_archive_member:{name}")
    if name.endswith((".pyc", ".pyo", ".safetensors")):
        raise ValueError(f"generated_archive_member:{name}")
    if re.search(rb"sk-or-v1-[A-Za-z0-9]{20,}", data):
        raise ValueError(f"credential_like_archive_content:{name}")
    if re.search(rb"/Users/[A-Za-z0-9._-]+/", data):
        raise ValueError(f"personal_absolute_path_in_archive:{name}")


def archive_members(path: Path) -> dict[str, bytes]:
    if path.suffix == ".whl":
        with zipfile.ZipFile(path) as archive:
            names = [item.filename for item in archive.infolist() if not item.is_dir()]
            if len(names) != len(set(names)):
                raise ValueError("duplicate_archive_member")
            return {
                item.filename: archive.read(item)
                for item in archive.infolist()
                if not item.is_dir()
            }
    with tarfile.open(path, "r:gz") as archive:
        result = {}
        root_name = None
        for item in archive.getmembers():
            if item.issym() or item.islnk():
                raise ValueError("archive_links_not_allowed")
            if item.isfile():
                handle = archive.extractfile(item)
                if handle is None:
                    raise ValueError("archive_member_unreadable")
                parts = PurePosixPath(item.name).parts
                if len(parts) < 2 or parts[0] in ("/", ".", "..") or ".." in parts:
                    raise ValueError("unsafe_archive_path")
                if root_name is None:
                    root_name = parts[0]
                if parts[0] != root_name:
                    raise ValueError("multiple_archive_roots")
                name = str(PurePosixPath(*parts[1:]))
                if name in result:
                    raise ValueError("duplicate_archive_member")
                result[name] = handle.read()
        return result


def inspect_archive(path: Path) -> dict:
    members = archive_members(path)
    if not members:
        raise ValueError("empty_archive")
    wheel = path.suffix == ".whl"
    for name, data in members.items():
        parts = PurePosixPath(name).parts
        if not parts or ".." in parts or name.startswith("/"):
            raise ValueError("unsafe_archive_path")
        check_content(name, data)
        if wheel:
            if parts[0] != "signal_reducers" and not parts[0].endswith(".dist-info"):
                raise ValueError(f"unexpected_wheel_member:{name}")
        elif parts[0] not in ROOT_FILES | ROOT_DIRECTORIES:
            raise ValueError(f"unexpected_source_member:{name}")
    dataset_name = (
        "signal_reducers/data/routing.jsonl"
        if wheel
        else "src/signal_reducers/data/routing.jsonl"
    )
    data = members[dataset_name]
    if hashlib.sha256(data).hexdigest() != DATASET_SHA256:
        raise ValueError("routing_dataset_changed")
    rows = [json.loads(line) for line in data.splitlines() if line.strip()]
    if Counter(row["split"] for row in rows) != {"development": 240, "heldout": 80}:
        raise ValueError("routing_dataset_split_changed")
    metadata_name = (
        next(name for name in members if name.endswith("/METADATA"))
        if wheel
        else "PKG-INFO"
    )
    metadata = members[metadata_name].decode()
    if (
        "Version: 0.1.0a1\n" not in metadata
        or "License-Expression: 0BSD\n" not in metadata
    ):
        raise ValueError("release_metadata_mismatch")
    if "License-File: LICENSE\n" not in metadata:
        raise ValueError("release_license_missing")
    return {
        "archive": path.name,
        "members": len(members),
        "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
        "dataset_sha256": DATASET_SHA256,
    }


def checked_run(
    command: list[str], *, cwd: Path, env: dict, label: str
) -> subprocess.CompletedProcess:
    result = subprocess.run(
        command, cwd=cwd, env=env, capture_output=True, text=True, timeout=120
    )
    if result.returncode:
        raise ValueError(f"installed_smoke_failed:{label}:exit_{result.returncode}")
    return result


def installed_smoke(path: Path) -> None:
    with tempfile.TemporaryDirectory(prefix="signal-reducers-release-") as directory:
        root = Path(directory)
        environment = root / "venv"
        # Reuse frozen native dependencies, while installing the project afresh.
        # Symlinks preserve the relocatable CPython runtime's native library paths.
        venv.EnvBuilder(with_pip=False, symlinks=True).create(environment)
        site_packages = Path(
            sysconfig.get_path(
                "purelib", vars={"base": str(environment), "platbase": str(environment)}
            )
        )
        site_packages.mkdir(parents=True, exist_ok=True)
        (site_packages / "_release_dependencies.pth").write_text(
            sysconfig.get_path("purelib") + "\n"
        )
        python = str(environment / "bin/python")
        uv = shutil.which("uv")
        if uv is None:
            raise ValueError("uv_required_for_installed_smoke")
        env = {
            key: value
            for key, value in os.environ.items()
            if key
            not in {"PYTHONPATH", "OPENROUTER_API_KEY", "OPENROUTER_KEY", "VIRTUAL_ENV"}
        }
        env.update(
            {
                "PIP_NO_INDEX": "1",
                "HF_HUB_OFFLINE": "1",
                "TRANSFORMERS_OFFLINE": "1",
                "UV_OFFLINE": "1",
                "UV_CACHE_DIR": str(root / "uv-cache"),
            }
        )
        checked_run(
            [
                uv,
                "pip",
                "install",
                "--python",
                python,
                "--no-deps",
                "--no-build-isolation",
                "--offline",
                str(path.resolve()),
            ],
            cwd=root,
            env=env,
            label="install",
        )
        for command in (
            [python, "-m", "signal_reducers"],
            [str(environment / "bin/signal-reducers")],
        ):
            version = checked_run(
                [*command, "--version"], cwd=root, env=env, label="version"
            )
            if version.stdout.strip() != "signal-reducers 0.1.0a1":
                raise ValueError("installed_version_mismatch")
            checked_run([*command, "--help"], cwd=root, env=env, label="help")
        inspection = checked_run(
            [
                python,
                "-c",
                "import hashlib,json,sys; from pathlib import Path; from importlib.resources import files; "
                "import signal_reducers; "
                "assert Path(signal_reducers.__file__).resolve().is_relative_to(Path(sys.prefix).resolve()); "
                "data=files('signal_reducers').joinpath('data/routing.jsonl').read_bytes(); "
                f"assert hashlib.sha256(data).hexdigest() == {DATASET_SHA256!r}; "
                "assert len(data.splitlines()) == 320",
            ],
            cwd=root,
            env=env,
            label="packaged_resources",
        )
        assert not inspection.stdout
        output = root / "fixture"
        checked_run(
            [
                python,
                "-m",
                "signal_reducers",
                "demo",
                "--mode",
                "fixture",
                "--output",
                str(output),
            ],
            cwd=root,
            env=env,
            label="fixture_demo",
        )
        summary = json.loads((output / "summary.json").read_text())
        if not summary["passed"] or summary["lifecycle_violations"]:
            raise ValueError("installed_fixture_failed")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("directory", type=Path, nargs="?", default=Path("dist"))
    parser.add_argument("--archives-only", action="store_true")
    args = parser.parse_args()
    artifacts = sorted(
        [
            *args.directory.glob("*.whl"),
            *args.directory.glob("signal_reducers-*.tar.gz"),
        ]
    )
    if len(artifacts) != 2 or sum(path.suffix == ".whl" for path in artifacts) != 1:
        raise ValueError("expected_one_wheel_and_one_sdist")
    for path in artifacts:
        result = inspect_archive(path)
        if not args.archives_only:
            installed_smoke(path)
        print(
            json.dumps(
                {**result, "installed_smoke": not args.archives_only, "passed": True}
            )
        )
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (
        OSError,
        ValueError,
        KeyError,
        StopIteration,
        subprocess.TimeoutExpired,
    ) as error:
        # Archive errors name paths and checks, never matched credential contents.
        print(json.dumps({"passed": False, "error": str(error)}), file=sys.stderr)
        raise SystemExit(1) from None
