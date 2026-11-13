import os
import subprocess
import sys

import pytest


@pytest.mark.parametrize("name", ["PLATFORM_AUTH_ENABLED", "TELEGRAM_NOTIFICATIONS_ENABLED"])
def test_invalid_security_flag_stops_startup_instead_of_silently_changing_mode(name):
    result = subprocess.run(
        [sys.executable, "-c", "from app.config import settings"],
        env={**os.environ, name: "misspelled"}, capture_output=True, timeout=10,
    )
    assert result.returncode != 0
    assert f"{name} must be a boolean" in result.stderr.decode()
