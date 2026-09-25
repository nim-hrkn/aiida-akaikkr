"""HTML report (docs/report_spec.md): CLI / MCP surface without a daemon, and a report of an existing chain when
the profile and the node are available (skipped otherwise)."""
import inspect
import os

import pytest

aiida = pytest.importorskip("aiida")


def test_report_subcommand_and_mcp_tool():
    from aiida_akaikkr.cli.spec import SUBCOMMANDS, READ
    from aiida_akaikkr.mcp import server

    spec = SUBCOMMANDS["report"]
    assert spec["kind"] == READ
    for key in ("pk", "lang", "embed", "dos_pk", "spc_pk", "jij_pk", "cnd_pk", "gaes_pk", "outdir", "prefix"):
        assert key in spec["options"], key
    assert server.TOOLS["kkr_report"] == "report" and "kkr_report" in server.tool_functions()   # read-only tool
    sig = inspect.signature(server.kkr_report)
    assert {"pk", "lang", "embed", "gaes_pk"} <= set(sig.parameters)


def test_report_style_and_languages():
    from pyakaikkr.report import LANGS, T
    from aiida_akaikkr.plot import STYLE
    from pyakaikkr.plot import DEFAULT_STYLE

    assert LANGS == ("en", "ja") and set(T["ja"]) == set(T["en"]) and set(STYLE) <= set(DEFAULT_STYLE)


@pytest.fixture(scope="module")
def profile():
    try:
        return aiida.load_profile(allow_switch=True)
    except Exception as e:  # noqa: BLE001
        pytest.skip("no AiiDA profile: {}".format(e))


def _finished_chain(orm):
    from aiida.orm import QueryBuilder
    q = QueryBuilder().append(orm.WorkChainNode, filters={"attributes.process_label": "AkaikkrChainWorkChain",
                                                          "attributes.exit_status": 0}, project=["id"]).order_by({orm.WorkChainNode: {"id": "desc"}})
    hits = q.first()
    return hits[0] if hits else None


def test_report_of_an_existing_chain(profile, tmp_path):
    from aiida import orm
    from aiida_akaikkr.report import collect_report_data, write_report

    pk = _finished_chain(orm)
    if pk is None:
        pytest.skip("no finished AkaikkrChainWorkChain in the profile")
    data = collect_report_data(pk, lang="ja")
    assert data.formula and data.scf.get("total_energy_Ry") is not None and data.components
    assert any(p["step"] == "go" and p["pk"] for p in data.provenance)
    assert "dos" in [f.name for f in data.figures]
    out = write_report(pk, outdir=str(tmp_path), lang="en", embed="svg")
    assert os.path.isfile(out["html"]) and out["summary"]["formula"] == data.formula
    page = open(out["html"], encoding="utf-8").read()
    assert '<html lang="en"' in page and "<svg" in page and data.formula in page
