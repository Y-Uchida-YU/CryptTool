import importlib.util
from pathlib import Path
from types import ModuleType


def load_script() -> ModuleType:
    path = Path(__file__).parents[2] / "scripts" / "write_build_verification.py"
    spec = importlib.util.spec_from_file_location("write_build_verification", path)
    if spec is None or spec.loader is None:
        raise RuntimeError("cannot load build verification script")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_junit_totals_aggregates_pytest_testsuites_root(tmp_path: Path) -> None:
    junit = tmp_path / "pytest.xml"
    junit.write_text(
        '<testsuites><testsuite tests="280" failures="0" errors="0" skipped="1"/>'
        '<testsuite tests="4" failures="1" errors="0" skipped="0"/></testsuites>',
        encoding="utf-8",
    )
    assert load_script().junit_totals(junit) == {
        "tests": 284,
        "failures": 1,
        "errors": 0,
        "skipped": 1,
    }
