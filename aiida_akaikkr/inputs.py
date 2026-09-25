# Copyright (c) 2022-2026 Hiori Kino.
# Distributed under the terms of the Apache License, Version 2.0.
"""Shared input construction: common parameters from a CIF, split into structure/parameters, builders.

Used by cli/steps.py and workflows/chain.py. Imports aiida at module level, so do not import
this module from the CLI spec or the MCP server.
"""
import os
import tempfile

from aiida import orm
from aiida.engine import calcfunction
from aiida.plugins import CalculationFactory

from .presets import MATERIALS, MODES, STRUCTURE_KEYS, mode_parameter_overrides

GENERIC = "generic"


def local_specx_for(code) -> str:
    """specx executable to run *locally* for the geometry step of make_common_param.

    The structure conversion (CIF -> AkaiKKR bravais parameters) runs specx once in a temporary
    directory on the machine where the calcfunction executes (the daemon host), so the path of a
    code on a remote computer (e.g. specx-akaikkr@mygardenx1-async) cannot be used. Order:
    the code's own executable when it exists here; else the environment variable
    AKAIKKR_LOCAL_SPECX; else a code with the same label on a computer with the core.local
    transport; else a clear error.
    """
    exe = str(code.filepath_executable)
    if os.path.isfile(exe):
        return exe
    env = os.environ.get("AKAIKKR_LOCAL_SPECX")
    if env and os.path.isfile(env):
        return env
    from aiida.orm import QueryBuilder
    for other in QueryBuilder().append(orm.InstalledCode, filters={"label": code.label}).all(flat=True):
        try:
            if other.computer.transport_type == "core.local" and os.path.isfile(str(other.filepath_executable)):
                return str(other.filepath_executable)
        except Exception:  # noqa: BLE001
            continue
    raise FileNotFoundError(
        f"specx of code {code.full_label} is not available on this machine ({exe}); the structure "
        "conversion needs a local specx: set AKAIKKR_LOCAL_SPECX or register a code with the same label "
        "on a core.local computer")


@calcfunction
def make_common_param(func_name: orm.Str, code: orm.InstalledCode, cif: orm.SinglefileData,
                      displc: orm.Bool, magtype: orm.Str) -> orm.Dict:
    """AkaiKKR common parameters from a CIF stored as SinglefileData.

    func_name: a `_<name>_common_param` function of akaikkr_testscript.testrun_class, or "generic"
    (structure from get_kkr_struc_from_cif, magtyp from `magtype`, everything else default).
    specx (`code`) runs once in geometry mode inside a temporary directory.
    """
    from akaikkr_testscript import get_kkr_struc_from_cif
    from akaikkr_testscript import testrun_class as trc

    exe = local_specx_for(code)
    with tempfile.TemporaryDirectory() as tmpdir:
        ciffilepath = os.path.join(tmpdir, cif.filename)
        with open(ciffilepath, "w") as f:
            f.write(cif.get_content())
        geomdir = os.path.join(tmpdir, "geom")
        if func_name.value == GENERIC:
            param = get_kkr_struc_from_cif(ciffilepath=ciffilepath, akaikkr_exe=exe, displc=displc.value,
                                           use_bravais=True, remove_temperaryfiles=True, directory=geomdir)
            param["magtyp"] = magtype.value or "nmag"
        else:
            func = getattr(trc, func_name.value)
            param = func(akaikkr_exe={"specx": exe, "fmg": "", "backend": "cif"}, displc=displc.value,
                         ciffilepath=ciffilepath, use_bravais=True, remove_temperaryfiles=True,
                         directory=geomdir)
            if magtype.value:
                param["magtyp"] = magtype.value
    return orm.Dict(dict=param)


@calcfunction
def split_param(common: orm.Dict, mode: orm.Str, fspin: orm.Float, from_potential: orm.Bool,
                overrides: orm.Dict) -> dict:
    """split common parameters into `structure` / `parameters` / `magtype` for one mode."""
    from pyakaikkr import AkaikkrJob

    common = common.get_dict()
    structure = {k: v for k, v in common.items() if k in STRUCTURE_KEYS}
    parameters = dict(AkaikkrJob("dummy").default)
    parameters.update({k: v for k, v in common.items() if k not in STRUCTURE_KEYS})
    parameters.update(mode_parameter_overrides(mode.value, fspin.value, from_potential.value))
    parameters.update(overrides.get_dict())
    parameters.pop("go", None)
    magtyp = parameters.pop("magtyp", "nmag")
    return {"structure": orm.Dict(dict=structure), "parameters": orm.Dict(dict=parameters),
            "magtype": orm.Str(magtyp)}


def build_calcjob(code, mode, common, *, fspin=None, from_potential=False, overrides=None,
                  potential=None, structure_data=None, displc=False, ncores=8, wallclock=7200, label=None):
    """builder of the specx CalcJob of `mode` from the common-parameter Dict node."""
    if mode not in MODES:
        raise ValueError(f"unknown mode {mode!r}; known: {list(MODES)}")
    entry_point, go = MODES[mode]
    parts = split_param(common, orm.Str(mode), orm.Float(fspin or 0.0), orm.Bool(bool(from_potential)),
                        orm.Dict(dict=overrides or {}))
    builder = CalculationFactory(entry_point).get_builder()
    builder.code = code
    builder.go = orm.Str(go)
    builder.magtype = parts["magtype"]
    builder.displc = orm.Bool(bool(displc))
    builder.structure = parts["structure"]
    builder.parameters = parts["parameters"]
    if mode == "fsm":
        builder.fspin = orm.Float(fspin if fspin is not None else 1.0)
    if potential is not None:
        builder.potential = potential
    if mode == "spc":
        if structure_data is None:
            raise ValueError("spc needs structure_data (the `structure` output of a go)")
        builder.structure_data = structure_data
    if label:
        builder.metadata.label = label
    builder.metadata.options.resources = {"num_machines": 1, "num_mpiprocs_per_machine": 1,
                                          "num_cores_per_mpiproc": int(ncores)}
    builder.metadata.options.max_wallclock_seconds = int(wallclock)
    builder.metadata.options.withmpi = False
    return builder


def preset_common_param(name: str, code, displc: bool, cif_path: str | None = None, magtype: str = ""):
    """common-parameter Dict node of a preset (its CIF unless `cif_path` is given)."""
    from .presets import preset_cif_path

    if name not in MATERIALS:
        raise ValueError(f"unknown preset {name!r}; known: {list(MATERIALS)}")
    path = os.path.abspath(cif_path or preset_cif_path(name))
    cif = orm.SinglefileData(file=path)
    return make_common_param(orm.Str(MATERIALS[name]["func"]), code, cif, orm.Bool(bool(displc)), orm.Str(magtype))


def generic_common_param(cif_path: str, code, displc: bool, magtype: str = "nmag"):
    cif = orm.SinglefileData(file=os.path.abspath(cif_path))
    return make_common_param(orm.Str(GENERIC), code, cif, orm.Bool(bool(displc)), orm.Str(magtype))


def summarize_common(node) -> dict:
    d = node.get_dict()
    return {"pk": node.pk, "brvtyp": d.get("brvtyp"), "a": d.get("a"), "ntyp": d.get("ntyp"),
            "natm": d.get("natm"), "types": d.get("type"), "magtyp": d.get("magtyp"),
            "has_displc": "displc" in d}


@calcfunction
def single_site_common_param(comp: orm.Str, brvtyp: orm.Str, lattice: orm.Str, magtyp: orm.Str) -> orm.Dict:
    """common-parameter Dict of a single-site CPA (one type, one atom) from a composition string
    such as "Rh0.5Pt0.5" or "AlSiScTi" (equal fractions), with the 2019 HEA defaults of
    pyakaikkr.gaes.make_single_site_param (a=1000000 for lattice "expr")."""
    from pyakaikkr.gaes import SiteComposition, make_single_site_param

    c = SiteComposition.from_type_name(comp.value)
    lat = lattice.value
    lat = lat if lat in ("expr", "mjw") else float(lat)
    param = make_single_site_param(c, brvtyp.value, lattice=lat, magtyp=magtyp.value or "mag")
    param.pop("go", None)
    return orm.Dict(dict=param)
