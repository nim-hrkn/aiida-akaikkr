"""GAES tests (layer 1 + layer 2): the CLI / MCP surface, the WorkChain spec, defaults, judgement and plot
helpers with aiida imported; two tests create orm nodes and need a loaded profile (skipped otherwise). No daemon.

An end-to-end run needs a profile and a code: see docs/gaes_workchain.md (`submit-gaes --comp Cu ...`,
verified on mygardenx2-slurm and mygardenx1-async, pks 3730 / 3021).
"""
import inspect

import numpy as np
import pytest

aiida = pytest.importorskip("aiida")


@pytest.fixture(scope="module")
def profile():
    """a loaded AiiDA profile (needed to create orm nodes); skipped where none is configured."""
    try:
        return aiida.load_profile(allow_switch=True)
    except Exception as e:  # noqa: BLE001
        pytest.skip("no AiiDA profile: {}".format(e))


# ---------------------------------------------------------------- CLI / MCP surface (no aiida)
def test_submit_gaes_spec_and_mcp_tool_expose_the_gaes_options():
    from aiida_akaikkr.cli.spec import SUBCOMMANDS
    from aiida_akaikkr.mcp import server

    opts = SUBCOMMANDS["submit-gaes"]["options"]
    for key in ("comp", "polytyp", "ewidth_init", "method", "dosth", "dosth2", "min_ewidth", "max_ewidth", "orbital",
                "max_ew", "ewidth_dos", "ref", "code"):
        assert key in opts, key
    sig = inspect.signature(server.kkr_submit_gaes)
    assert {"comp", "orbital", "min_ewidth", "max_ewidth", "code"} <= set(sig.parameters)
    assert "gaes_pk" in inspect.signature(server.kkr_plot).parameters
    assert "gaes_pk" in SUBCOMMANDS["plot"]["options"]


def test_submit_gaes_orbital_argument_is_split_into_a_list(monkeypatch, profile):
    """steps.submit_gaes turns "Rb4p=valence,Bi6s=core" into gaes["orbitals"] (checked through the dict it
    builds; the submission itself is monkeypatched away)."""
    from aiida import orm
    from aiida_akaikkr.cli import steps

    captured = {}

    class FakeCode:
        label = "specx-akaikkr"
        full_label = "specx-akaikkr@x"
        pk = 1

    class FakeNode:
        pk = 99
        process_label = "AkaikkrGaesWorkChain"

    def fake_submit(cls, **inputs):
        captured.update(inputs)
        return FakeNode()

    monkeypatch.setattr(steps, "_load_code", lambda code: FakeCode())
    monkeypatch.setattr(steps, "_node", lambda pk: orm.Dict(dict={"natm": 1}))
    monkeypatch.setattr("aiida.engine.submit", fake_submit)
    monkeypatch.setattr(steps.logdir, "append_jsonl", lambda *a, **k: None)
    monkeypatch.setattr(orm.Dict, "store", lambda self: self, raising=False)
    info = steps.submit_gaes(structure_pk=1, orbital="Rb4p=valence, Bi6s=core", min_ewidth=0.5, code="specx-akaikkr@x")
    assert info["gaes"]["orbitals"] == ["Rb4p=valence", "Bi6s=core"] and info["gaes"]["min_ewidth"] == 0.5
    assert captured["gaes"].get_dict()["orbitals"] == ["Rb4p=valence", "Bi6s=core"]


# ---------------------------------------------------------------- WorkChain spec and defaults
def test_workchain_spec_ports_and_exit_codes():
    from aiida_akaikkr.workflows.gaes import AkaikkrGaesWorkChain, GAES_DEFAULTS

    spec = AkaikkrGaesWorkChain.spec()
    assert {"code", "common", "gaes", "overrides", "displc", "ncores", "wallclock", "label"} <= set(spec.inputs.keys())
    assert {"ewidth", "status", "history", "parameters"} <= set(spec.outputs.keys())
    codes = {name: spec.exit_codes[name].status for name in spec.exit_codes.keys() if name.startswith("ERROR_")}
    assert codes["ERROR_GO_FAILED"] == 400 and codes["ERROR_DOS_FAILED"] == 401
    assert codes["ERROR_EWIDTH_FAIL"] == 420 and codes["ERROR_EWIDTH_EXHAUSTED"] == 421
    assert codes["ERROR_NO_CANDIDATE"] == 422 and codes["ERROR_ORBITAL_RULE"] == 423
    # defaults follow the spec (docs/gaes_workchain.md): per-atom judgement, bounds replaced by orbital rules
    assert GAES_DEFAULTS["method"] == 2 and GAES_DEFAULTS["dosth"] == 2e-2 and GAES_DEFAULTS["dosth2"] == 1e-3
    assert GAES_DEFAULTS["eth"] == 0.3 and GAES_DEFAULTS["ediff"] == 0.2 and GAES_DEFAULTS["ewidth_init"] == 1.2
    assert GAES_DEFAULTS["min_ewidth"] is None and GAES_DEFAULTS["max_ewidth"] is None
    assert GAES_DEFAULTS["orbitals"] == [] and GAES_DEFAULTS["dos_per_atom"] is True
    assert GAES_DEFAULTS["edelt_dos"] == 1e-4 and GAES_DEFAULTS["ewidth_dos_max"] == 4.5
    assert "akaikkr.gaes" in _entry_points()


def _entry_points():
    from importlib.metadata import entry_points

    return {ep.name for ep in entry_points(group="aiida.workflows")}


def test_bounds_helper_uses_the_entry_then_the_parameters_then_the_defaults():
    from aiida_akaikkr.plot import _gaes_bounds

    assert _gaes_bounds({"min_ewidth": None, "max_ewidth": None}) == (1.0, 2.0)
    assert _gaes_bounds({"min_ewidth": 0.5, "max_ewidth": 1.5}) == (0.5, 1.5)
    assert _gaes_bounds({"min_ewidth": 0.5, "max_ewidth": 1.5}, {"orbital_bounds": [None, 0.9]}) == (None, 0.9)
    assert _gaes_bounds({"min_ewidth": None, "max_ewidth": None}, {"orbital_bounds": []}) == (1.0, 2.0)


def test_workchain_judgement_matches_pyakaikkr_per_atom_rule():
    """the WorkChain's judge divides the DOS by natm before pyakaikkr.gaes.decide (same rule as Gaes)."""
    from pyakaikkr.gaes import decide, per_atom

    e = np.linspace(-2.2425, 0.7425, 400)
    d = np.full_like(e, 3e-3)                       # 5-atom cell: gap floor 3e-3 per cell = 6e-4 per atom
    d[e > -0.9] = 30.0
    d[(e > -1.75) & (e < -1.55)] = 200.0
    assert decide(2, e, [d], 1.2, dosth=2e-2, dosth2=1e-3).flag == "fail"
    assert decide(2, e, per_atom([d], 5), 1.2, dosth=2e-2, dosth2=1e-3, min_ewidth=1.0, max_ewidth=2.0).flag == "old"


def test_spin_sum_of_dos_arrays(profile):
    from aiida import orm
    from aiida_akaikkr.workflows.gaes import _spin_sum

    node = orm.ArrayData()
    node.set_array("energy", np.linspace(-1, 1, 5))
    node.set_array("dos", np.array([[1.0, 2, 3, 4, 5], [1.0, 1, 1, 1, 1]]))
    energy, curve = _spin_sum(node)
    assert energy.shape == (5,) and list(curve) == [2, 3, 4, 5, 6]


# ---------------------------------------------------------------- structure generation with a remote code
def test_local_specx_for_prefers_existing_code_path_then_env(monkeypatch, tmp_path):
    from aiida_akaikkr.inputs import local_specx_for

    class Code:
        label = "specx-akaikkr"
        full_label = "specx-akaikkr@remote"

        def __init__(self, path):
            self.filepath_executable = path

    local = tmp_path / "specx"
    local.write_text("#!/bin/sh\n")
    assert local_specx_for(Code(str(local))) == str(local)
    monkeypatch.setenv("AKAIKKR_LOCAL_SPECX", str(local))
    assert local_specx_for(Code("/nonexistent/on/this/host/specx")) == str(local)


# ---------------------------------------------------------------- figures: aiida palette is a pyakaikkr style
def test_plot_style_is_a_valid_pyakaikkr_style_and_plot_cli_needs_a_pk():
    from pyakaikkr.plot import DEFAULT_STYLE
    from aiida_akaikkr import plot

    assert set(plot.STYLE) <= set(DEFAULT_STYLE)
    with pytest.raises(ValueError):
        plot.plot_cli()
