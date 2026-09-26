"""Layer-1 tests: no aiida needed. Structure of the CLI spec and the MCP server."""
import inspect
import os
import subprocess
import sys

import pytest

HERE = os.path.dirname(os.path.abspath(__file__))
SERVER_PY = os.path.join(HERE, "..", "aiida_akaikkr", "mcp", "server.py")


def test_mcp_server_does_not_import_aiida():
    code = ("import sys; import aiida_akaikkr.mcp.server as s; "
            "print(sorted(m for m in sys.modules if m.split('.')[0] in ('aiida', 'pyakaikkr', 'pymatgen')))")
    out = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, check=True).stdout.strip()
    assert out == "[]", out


def test_no_verdi_in_server():
    src = open(SERVER_PY).read()
    assert "verdi" not in src.replace("`verdi computer test`", "")


def test_allowlist_is_the_spec():
    from aiida_akaikkr.cli.spec import SUBCOMMANDS
    from aiida_akaikkr.mcp import server

    assert server.ALLOWED_SUBCOMMANDS == frozenset(SUBCOMMANDS)
    assert set(server.TOOLS.values()) == set(SUBCOMMANDS), "every subcommand has exactly one tool"
    assert server.ALLOWED_BINARY_NAMES == ("akaikkr-aiida",)


def test_more_than_ten_tools_and_flags_filter():
    from aiida_akaikkr.mcp import server

    assert len(server.TOOLS) > 10
    read_only = server.tool_functions()
    assert "kkr_submit_go" not in read_only and "kkr_kill" not in read_only and "kkr_status" in read_only
    assert "kkr_submit_go" in server.tool_functions(allow_submit=True)
    assert "kkr_kill" in server.tool_functions(allow_control=True)


def test_every_tool_argument_reaches_argv(monkeypatch):
    """the failure to guard against: a tool argument that is silently dropped from argv."""
    from aiida_akaikkr.cli.spec import SUBCOMMANDS, option_flag
    from aiida_akaikkr.mcp import server

    monkeypatch.setenv("AKAIKKR_AIIDA_BIN", "/x/akaikkr-aiida")
    for tool, sub in server.TOOLS.items():
        fn = getattr(server, tool)
        params = [p for p in inspect.signature(fn).parameters if p != "self"]
        options = SUBCOMMANDS[sub]["options"]
        assert set(params) == set(options), f"{tool}: parameters {params} != spec options {list(options)}"
        sample = {}
        for name, (typ, _req, _help) in options.items():
            sample[name] = {"int": 7, "float": 1.5, "str": "x", "bool": True}[typ]
        argv = server.build_argv(sub, sample)
        assert argv[0] == "/x/akaikkr-aiida" and argv[1] == "--json" and sub in argv
        for name, (typ, _req, _help) in options.items():
            assert option_flag(name) in argv, f"{tool}: {name} not passed"
            if typ != "bool":
                assert argv[argv.index(option_flag(name)) + 1] == str(sample[name])


def test_unknown_subcommand_or_argument_rejected(monkeypatch):
    from aiida_akaikkr.mcp import server

    monkeypatch.setenv("AKAIKKR_AIIDA_BIN", "/x/akaikkr-aiida")
    with pytest.raises(ValueError):
        server.build_argv("node-delete", {})
    with pytest.raises(ValueError):
        server.build_argv("status", {"pk": 1})


def test_binary_name_guard(monkeypatch):
    from aiida_akaikkr.mcp import server

    monkeypatch.setenv("AKAIKKR_AIIDA_BIN", "/usr/bin/verdi")
    with pytest.raises(RuntimeError):
        server.binary()


def test_cli_parser_builds_and_json_failure_is_json():
    from aiida_akaikkr.cli.main import build_parser

    parser = build_parser()
    args = parser.parse_args(["--json", "process", "--pk", "5"])
    assert args.command == "process" and args.pk == 5 and args.json
    proc = subprocess.run([sys.executable, "-m", "aiida_akaikkr.cli.main", "--json", "--profile", "no-such-profile",
                           "status"], capture_output=True, text=True)
    assert proc.returncode == 1
    import json
    out = json.loads(proc.stdout.strip().splitlines()[-1])
    assert out["ok"] is False and "error" in out


def test_comp_and_overrides_options():
    """submit-go accepts a composition instead of a structure pk; submit-chain takes overrides and the spc structure."""
    from aiida_akaikkr.cli import steps
    from aiida_akaikkr.cli.spec import SUBCOMMANDS
    from aiida_akaikkr.mcp import server

    for sub in ("submit-go", "submit-chain", "submit-gaes"):
        opts = SUBCOMMANDS[sub]["options"]
        assert {"comp", "polytyp", "lattice", "magtype", "parameters"} <= set(opts)
        assert opts["structure_pk"][1] is False, sub + ": structure_pk must be optional"
    assert "spc_structure_pk" in SUBCOMMANDS["submit-chain"]["options"]
    assert {"comp", "spc_structure_pk", "parameters"} <= set(inspect.signature(server.kkr_submit_chain).parameters)
    assert "comp" in inspect.signature(server.kkr_submit_go).parameters
    argv = server.build_argv("submit-chain", {"comp": "AlSiRhBi", "parameters": '{"bzqlty": 6}', "spc_structure_pk": 12})
    assert argv[-6:] == ["--comp", "AlSiRhBi", "--spc-structure-pk", "12", "--parameters", '{"bzqlty": 6}']
    with pytest.raises(ValueError, match="--structure-pk or --comp"):
        steps._common_from(None, what="--structure-pk or --comp")
