"""Run the AkaiKKR test-set materials through AiiDA (go, then dos/spc/tc/j3.0/fsm).

The per-material parameters are taken from the AkaiKKR test script
(`akaikkr_testscript.testrun_class._<name>_common_param`), so the results can
be compared with `tests/testrun/akaikkr/reference/*.json` of AkaiKKRPythonUtil.

Usage (inside the `akaikkr` conda env):

    python example/run_examples.py [--code specx-akaikkr@mygardenx2-slurm] [--only Fe,Co] [--ncores 8] [--figdir example/figures]

With --figdir, DOS / PDOS / A(w,k) / J_ij figures (and <name>_jij.csv) are written for every finished
dos / spc / j3.0 CalcJob after the run (same as `example/plot_results.py --dos/--spc/--jij`).

The structure step runs specx locally in "geometry" mode inside a temporary
directory; the CIF enters the provenance as a SinglefileData node.
A JSON summary (pks, exit codes, total energies, moments) is written to
run_examples_result.json in the current directory.
"""
import argparse
import json
import os
import tempfile
import time

from aiida import load_profile, orm
from aiida.engine import calcfunction, submit
from aiida.plugins import CalculationFactory

from akaikkr_testscript import testrun_class as trc
from pyakaikkr import AkaikkrJob

STRUCTURE_KEYS = {"brvtyp", "a", "c/a", "b/a", "alpha", "beta", "gamma", "r1", "r2", "r3",
                  "ntyp", "type", "ncmp", "rmt", "field", "mxl", "anclr", "conc",
                  "natm", "atmicx", "displc"}

# name: (common_param function, cif, modes after go, fspin for fsm)
MATERIALS = {
    "Cu":           ("_Cu_common_param",           "Cu-Fm3m.cif",        ["dos", "spc"], None),
    "Fe":           ("_Fe_common_param",           "Fe-Im3m.cif",        ["fsm", "tc", "j3.0", "dos", "spc"], 1.0),
    "Co":           ("_Co_common_param",           "Co_P63mmc.cif",      ["fsm", "tc", "j3.0", "dos", "spc"], 1.0),
    "Ni":           ("_Ni_common_param",           "Ni-Fm3m.cif",        ["fsm", "tc", "j3.0", "dos", "spc"], 1.0),
    "NiFe":         ("_NiFe_common_param",         "NiFe-Fm3m.cif",      ["fsm", "tc", "j3.0", "dos", "spc"], 1.0),
    "FeRh05Pt05":   ("_FeRh05Pt05_common_param",   "FeRh0.5Pt0.5.cif",   ["fsm", "tc", "j3.0", "dos", "spc"], 3.0),
    "AlMnFeCo_bcc": ("_AlMnFeCo_bcc_common_param", "AlMnFeCo-Im3m.cif",  ["fsm", "tc", "j3.0", "dos", "spc"], 1.0),
    "Fe_lmd":       ("_Fe_lmd_common_param",       "Fe-Im3m.cif",        ["dos", "spc"], None),
    "FeB195":       ("_FeB195_common_param",       "FeB1.95-P6mmm.cif",  ["dos", "spc"], None),
    "GaAs":         ("_GaAs_common_param",         "GaAsVc-F43m.cif",    ["dos", "spc"], None),
    "Co2MnSi":      ("_Co2MnSi_common_param",      "Co2MnSi-Fm3m.cif",   ["fsm", "tc", "j3.0", "dos", "spc"], 4.5),
    "SmCo5_oc":     ("_SmCo5_oc_common_param",     "SmCo5_P6mmm.cif",    ["fsm", "tc", "j3.0", "dos", "spc"], 6.5),
    "SmCo5_noc":    ("_SmCo5_noc_common_param",    "SmCo5_P6mmm.cif",    [], None),
}
# spc of these materials takes the k-path structure from another material's go
SPC_STRUCTURE_FROM = {"Fe_lmd": "Fe"}
# fsm of these materials starts from the go potential (record=2nd), as in the test script
FSM_FROM_GO_POTENTIAL = {"FeRh05Pt05", "Co2MnSi", "SmCo5_oc"}

MODE_PLUGIN = {"go": "akaikkr.go", "dos": "akaikkr.dos", "spc": "akaikkr.spc",
               "tc": "akaikkr.tc", "j3.0": "akaikkr.jij", "fsm": "akaikkr.fsm", "cnd": "akaikkr.cnd"}
# materials with a cnd (conductivity) step in the akaikkr_cnd test set
CND_MATERIALS = {"NiFe", "FeRh05Pt05", "AlMnFeCo_bcc"}


@calcfunction
def make_common_param(func_name: orm.Str, code: orm.InstalledCode, cif: orm.SinglefileData,
                      displc: orm.Bool) -> orm.Dict:
    """structure/common parameters from the CIF (stored as SinglefileData) with the test-script helper.

    The geometry run of specx (`code`) happens in a temporary directory that is removed afterwards,
    so only the CIF content and the resulting Dict enter the provenance.
    """
    func = getattr(trc, func_name.value)
    akaikkr_exe = {"specx": str(code.filepath_executable), "fmg": "", "backend": "cif"}
    with tempfile.TemporaryDirectory() as tmpdir:
        ciffilepath = os.path.join(tmpdir, cif.filename)
        with open(ciffilepath, "w") as f:
            f.write(cif.get_content())
        param = func(akaikkr_exe=akaikkr_exe, displc=displc.value, ciffilepath=ciffilepath,
                     use_bravais=True, remove_temperaryfiles=True,
                     directory=os.path.join(tmpdir, "geom"))
    return orm.Dict(dict=param)


@calcfunction
def split_param(common: orm.Dict, mode: orm.Str, fspin: orm.Float) -> dict:
    """split the common parameters into structure / parameters for the given mode."""
    common = common.get_dict()
    structure = {k: v for k, v in common.items() if k in STRUCTURE_KEYS}
    parameters = dict(AkaikkrJob("dummy").default)
    parameters.update({k: v for k, v in common.items() if k not in STRUCTURE_KEYS})
    m = mode.value
    if m == "go":
        parameters["record"] = "init"
    elif m == "dos":
        parameters["record"] = "2nd"
        parameters["ewidth"] = 2.0
    elif m in ("tc", "j3.0", "spc"):
        parameters["record"] = "2nd"
    elif m == "cnd":
        parameters["record"] = "2nd"
        parameters["ewidth"] = 0.01
        parameters["bzqlty"] = 40
    elif m == "fsm":
        parameters["record"] = "2nd" if fspin.value < 0 else "init"
        parameters["fspin"] = abs(fspin.value)
        parameters["pmix"] = "0.02ch"
    parameters.pop("go", None)
    parameters.pop("magtyp", None)
    return {"structure": orm.Dict(dict=structure), "parameters": orm.Dict(dict=parameters),
            "magtype": orm.Str(common["magtyp"])}


def build(code, name, mode, common, fspin, potential, structure_data, ncores, wallclock, displc=False):
    parts = split_param(common, orm.Str(mode), orm.Float(fspin or 0.0))
    builder = CalculationFactory(MODE_PLUGIN[mode]).get_builder()
    builder.code = code
    builder.go = orm.Str({"spc": "spc31", "cnd": " cnd"}.get(mode, mode))
    builder.magtype = parts["magtype"]
    builder.displc = orm.Bool(displc)
    builder.structure = parts["structure"]
    builder.parameters = parts["parameters"]
    if mode == "fsm":
        builder.fspin = orm.Float(abs(fspin))
    if potential is not None:
        builder.potential = potential
    if mode == "spc":
        builder.structure_data = structure_data
    builder.metadata.label = f"{name}_{mode}"
    builder.metadata.options.resources = {
        "num_machines": 1, "num_mpiprocs_per_machine": 1, "num_cores_per_mpiproc": ncores}
    builder.metadata.options.max_wallclock_seconds = wallclock
    builder.metadata.options.withmpi = False
    return builder


def wait_all(nodes, poll=10):
    while any(not n.is_terminated for n in nodes.values()):
        time.sleep(poll)


def summarize(node):
    d = {"pk": node.pk, "exit": node.exit_status, "state": node.process_state.value}
    if "results" in node.outputs:
        r = node.outputs.results
        d["te"] = r.get("total_energy")
        d["moment"] = r.get("total_moment")
        d["converged"] = r.get("convervence")
    if "Tc" in node.outputs:
        d["Tc"] = node.outputs.Tc.value
    if "resistivity" in node.outputs:
        d["resis"] = node.outputs.resistivity.value
        d["cnd"] = node.outputs.conductivity.get_list()
    return d


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--code", default="specx-akaikkr@mygardenx2-slurm")
    ap.add_argument("--only", default="", help="comma separated material names")
    ap.add_argument("--ncores", type=int, default=8)
    ap.add_argument("--wallclock", type=int, default=7200)
    ap.add_argument("--structure-dir", default=os.path.join(os.path.dirname(os.path.abspath(__file__)), "structure"))
    ap.add_argument("--displc", action="store_true", help="add displc (akaikkr_cnd); also runs cnd for CND_MATERIALS")
    ap.add_argument("--result", default="run_examples_result.json")
    ap.add_argument("--figdir", default=None, help="write figures of the finished dos/spc/j3.0 jobs here")
    args = ap.parse_args()
    names = [n for n in args.only.split(",") if n] or list(MATERIALS)

    load_profile()
    code = orm.load_code(args.code)

    common = {}
    for name in names:
        func, cif, _, _ = MATERIALS[name]
        cifnode = orm.SinglefileData(file=os.path.abspath(os.path.join(args.structure_dir, cif)))
        common[name] = make_common_param(orm.Str(func), code, cifnode, orm.Bool(args.displc))
        print(f"{name}: structure ok, types={common[name]['type']}", flush=True)

    # ---- go for all materials
    go_nodes = {}
    for name in names:
        go_nodes[name] = submit(build(code, name, "go", common[name], None, None, None,
                                      args.ncores, args.wallclock, displc=args.displc))
        print(f"{name}_go submitted pk={go_nodes[name].pk}", flush=True)
    wait_all(go_nodes)
    for name, node in go_nodes.items():
        print(f"{name}_go finished: {summarize(node)}", flush=True)

    # ---- follow-up modes
    nodes = {}
    for name in names:
        go = go_nodes[name]
        if go.exit_status != 0:
            print(f"{name}: go failed, skipping follow-ups", flush=True)
            continue
        _, _, modes, fspin = MATERIALS[name]
        if args.displc and name in CND_MATERIALS:
            modes = list(modes) + ["cnd"]
        for mode in modes:
            structure_data = None
            if mode == "spc":
                src = SPC_STRUCTURE_FROM.get(name, name)
                src_go = go_nodes.get(src)
                if src_go is None or "structure" not in src_go.outputs:
                    print(f"{name}_spc skipped: no structure from {src}_go", flush=True)
                    continue
                structure_data = src_go.outputs.structure
            potential = go.outputs.potential
            if mode == "fsm":
                if name in FSM_FROM_GO_POTENTIAL:
                    fspin = -fspin  # negative sign = restart from the go potential (record=2nd)
                else:
                    potential = None
            nodes[f"{name}_{mode}"] = submit(build(code, name, mode, common[name], fspin, potential,
                                                   structure_data, args.ncores, args.wallclock,
                                                   displc=args.displc))
            print(f"{name}_{mode} submitted pk={nodes[f'{name}_{mode}'].pk}", flush=True)
    wait_all(nodes)

    result = {f"{n}_go": summarize(g) for n, g in go_nodes.items()}
    result.update({label: summarize(n) for label, n in nodes.items()})
    with open(args.result, "w") as f:
        json.dump(result, f, indent=1)
    print("\nSUMMARY", flush=True)
    for label, d in result.items():
        print(f"{label:24s} pk={d['pk']:<5d} exit={d['exit']}  te={d.get('te')}  moment={d.get('moment')}  Tc={d.get('Tc', '')}  resis={d.get('resis', '')}", flush=True)

    if args.figdir:
        from aiida_akaikkr.plot import plot_cli

        for name in names:
            pks = {m: nodes[f"{name}_{m}"].pk for m in ("dos", "spc", "j3.0")
                   if f"{name}_{m}" in nodes and nodes[f"{name}_{m}"].exit_status == 0}
            if not pks:
                continue
            for path in plot_cli(dos_pk=pks.get("dos"), spc_pk=pks.get("spc"), jij_pk=pks.get("j3.0"),
                                 outdir=args.figdir, prefix=name)["files"]:
                print("wrote", path, flush=True)


if __name__ == "__main__":
    main()
