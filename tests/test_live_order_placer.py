"""Test sanity-checking lib/live_order_placer.py imports cleanly pre-KYC.

These tests are designed to:
  - verify the module imports WITHOUT `py_clob_client` installed (lazy import)
  - verify LiveOrderCredentials.from_env() returns None when keys absent
  - verify LiveOrderPlacer construction raises LiveConfigurationError when creds incomplete
  - verify LiveOrderPlacer.place_quote raises LiveNotImplementedError pre-KYC
  - verify main_live.py CLI surfaces --phase-2a / --dry-run flags

These must run WITHOUT py_clob_client installed (the test machine is
pre-KYC). Post-KYC activation requires the user to populate .env + pip install
py_clob_client; lib/live_order_placer.connect() exercises a separate path.
"""
import os
import sys
import pytest
from pathlib import Path
ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from lib.live_order_placer import (  # noqa: E402
    LiveConfigurationError,
    LiveNotImplementedError,
    LiveOrderCredentials,
    LiveOrderPlacer,
)


def test_module_imports_without_py_clob_client():
    """The module must import WITHOUT `py_clob_client` installed.

    This is the pre-KYC base behavior; py_clob_client import is deferred
    to LiveOrderPlacer.connect() (lazy import strategy).
    """
    import importlib
    # ensure py_clob_client is not installed in the test env; if it is,
    # we skip because this scenario only matters when the dep is absent.
    if importlib.util.find_spec("py_clob_client") is not None:
        pytest.skip("py_clob_client is installed; pre-KYC import path is not exercised")
    # the import statement at the top of this test file already imports
    # live_order_placer — it must not have failed. we additionally
    # confirm the module is importable via importlib for isolation.
    mod = importlib.import_module("lib.live_order_placer")
    assert hasattr(mod, "LiveOrderPlacer")
    assert hasattr(mod, "LiveOrderCredentials")


def test_creds_from_env_returns_none_when_missing():
    """LiveOrderCredentials.from_env returns None when ANY required key
    is missing.  This is the pre-KYC scene on the oracle VM.
    """
    # pre-KYC env: simulate empty dict to avoid contaminating os.environ.
    no_creds_env = {}
    creds = LiveOrderCredentials.from_env(no_creds_env)
    assert creds is None, "from_env() must return None when required creds missing"


def test_creds_from_env_returns_creds_when_all_present():
    """LiveOrderCredentials.from_env returns a creds instance when all
    required keys are present.
    """
    test_env = {
        "POLY_L2_API_KEY": "test-key",
        "POLY_L2_API_SECRET": "test-secret",
        "POLY_L2_API_PASSPHRASE": "test-pass",
        "POLY_EVM_WALLET_PRIV_KEY": "0x" + "1" * 64,
        "POLY_PROXY_WALLET_ADDRESS": "0x" + "2" * 40,
    }
    creds = LiveOrderCredentials.from_env(test_env)
    assert creds is not None
    assert creds.l2_api_key == "test-key"
    assert creds.l2_api_secret == "test-secret"
    assert creds.l2_api_passphrase == "test-pass"
    assert creds.evm_wallet_priv_key.startswith("0x1")
    assert creds.proxy_wallet_address == "0x" + "2" * 40


def test_placer_construct_raises_when_creds_invalid():
    """Constructing LiveOrderPlacer with incomplete creds raises
    LiveConfigurationError (not AttributeError / ErrorCode).
    """
    bad_creds = LiveOrderCredentials(
        l2_api_key="",
        l2_api_secret="x",
        l2_api_passphrase="y",
        evm_wallet_priv_key="z",
    )
    with pytest.raises(LiveConfigurationError):
        LiveOrderPlacer(bad_creds)


def test_placer_place_quote_raises_pre_kyc():
    """LiveOrderPlacer.place_quote raises LiveNotImplementedError in the
    pre-KYC state — the body has not yet executed the py_clob_client call
    graph.
    """
    creds = LiveOrderCredentials(
        l2_api_key="test-key",
        l2_api_secret="test-secret",
        l2_api_passphrase="test-pass",
        evm_wallet_priv_key="0x" + "1" * 64,
    )
    placer = LiveOrderPlacer(creds)
    with pytest.raises(LiveNotImplementedError):
        placer.place_quote(q=None)


def test_placer_connect_raises_when_py_clob_client_missing():
    """When `py_clob_client` is not installed and connect() is called,
    LiveConfigurationError is raised with instructions to install it.
    """
    import importlib
    if importlib.util.find_spec("py_clob_client") is not None:
        pytest.skip("py_clob_client installed; missing-dep path is not exercised")
    creds = LiveOrderCredentials(
        l2_api_key="test-key",
        l2_api_secret="test-secret",
        l2_api_passphrase="test-pass",
        evm_wallet_priv_key="0x" + "1" * 64,
    )
    placer = LiveOrderPlacer(creds)
    with pytest.raises(LiveConfigurationError):
        placer.connect()


def test_main_live_help_exits_cleanly():
    """main_live.py --help must exit 0 cleanly without raising (declares CLI
    surface).  The point of this test is regression-catch CLI breakage after
    modifications; the CLI surfaces --phase-2a / --dry-run / --top-n flags
    documented in lib/live_order_placer.py.
    """
    import subprocess
    result = subprocess.run(
        [sys.executable, str(ROOT / "main_live.py"), "--help"],
        capture_output=True, text=True, cwd=str(ROOT),
    )
    assert result.returncode == 0
    out = result.stdout + result.stderr
    assert "--phase-2a" in out
    assert "--dry-run" in out
    assert "--top-n" in out


def test_main_live_no_args_exits_2():
    """main_live.py with no --phase-2a argument must exit 2 with a clear
    error (preventing accidental immediate-live start before KYC).
    """
    import subprocess
    result = subprocess.run(
        [sys.executable, str(ROOT / "main_live.py")],
        capture_output=True, text=True, cwd=str(ROOT),
    )
    assert result.returncode == 2
    assert "--phase-2a" in result.stderr
