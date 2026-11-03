from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
SAMPLE_FILES = [str(ROOT / "data/sample/BTCUSDT.csv"), str(ROOT / "data/sample/ETHUSDT.csv")]


@pytest.fixture
def sample_files():
    return SAMPLE_FILES
