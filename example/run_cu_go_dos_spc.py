"""Run AkaiKKR go -> dos -> spc31 for fcc Cu through AiiDA.

Usage (inside the `akaikkr` conda env, with the `akaikkr` profile):

    python example/run_cu_go_dos_spc.py [--code specx-akaikkr@mygardenx2-slurm] [--cif example/structure/Cu-Fm3m.cif]

The structure parameters are obtained with the same helper as the AkaiKKR
test script (`akaikkr_testscript.get_kkr_struc_from_cif`), which runs specx
once locally in "geometry" mode (in a temporary directory). The CIF is stored
as a SinglefileData node so its content is part of the provenance.  The three CalcJobs are submitted through
the daemon; the potential produced by `go` is passed to `dos` and `spc`
as a `SinglefileData` node.
"""
import argparse
import os
import tempfile

from aiida import load_profile, orm
from aiida.engine import calcfunction, submit
from aiida.plugins import CalculationFactory

from akaikkr_testscript import change_atomic_type, get_kkr_struc_from_cif
from pyakaikkr import AkaikkrJob


def wait_for_node_finished(node, poll=5):
    import time
    while not node.is_terminated:
        time.sleep(poll)
    return node


@calcfunction
def make_structure_param(code: orm.InstalledCode, cif: orm.SinglefileData, displc: orm.Bool) -> orm.Dict:
    """kkr structure parameters from the CIF (SinglefileData); specx (`code`) runs in geometry mode
    inside a temporary directory."""
    with tempfile.TemporaryDirectory() as tmpdir:
        ciffilepath = os.path.join(tmpdir, cif.filename)
        with open(ciffilepath, "w") as f:
            f.write(cif.get_content())
        param = get_kkr_struc_from_cif(
            ciffilepath=ciffilepath, akaikkr_exe=str(code.filepath_executable), displc=displc.value,
            use_bravais=True, remove_temperaryfiles=True, directory=os.path.join(tmpdir, "geom"))
    param = change_atomic_type(param, {0: "Cu"})
    return orm.Dict(dict=param)


@calcfunction
def make_go_param(record: orm.Str, ewidth: orm.Float) -> orm.Dict:
    kkr_param = dict(AkaikkrJob("dummy").default)
    kkr_param["record"] = record.value
    kkr_param["ewidth"] = ewidth.value
    return orm.Dict(dict=kkr_param)


def common_builder(code, calcname, structure, parameters, magtype, potential=None, ncores=8):
    builder = CalculationFactory(calcname).get_builder()
    builder.code = code
    builder.magtype = orm.Str(magtype)
    builder.displc = orm.Bool(False)
    builder.structure = structure
    builder.parameters = parameters
    if potential is not None:
        builder.potential = potential
    builder.metadata.options.resources = {
        "num_machines": 1, "num_mpiprocs_per_machine": 1, "num_cores_per_mpiproc": ncores}
    builder.metadata.options.max_wallclock_seconds = 1800
    builder.metadata.options.withmpi = False
    return builder


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--code", default="specx-akaikkr@mygardenx2-slurm")
    ap.add_argument("--cif", default=os.path.join(os.path.dirname(__file__), "structure", "Cu-Fm3m.cif"))
    ap.add_argument("--ncores", type=int, default=8)
    args = ap.parse_args()

    load_profile()
    code = orm.load_code(args.code)
    magtype = "nmag"

    structure = make_structure_param(
        code, orm.SinglefileData(file=os.path.abspath(args.cif)), orm.Bool(False))
    print("structure param:", structure.get_dict())

    # ---- go
    go_param = make_go_param(orm.Str("init"), orm.Float(1.0))
    builder = common_builder(code, "akaikkr.go", structure, go_param, magtype, ncores=args.ncores)
    builder.go = orm.Str("go")
    go_node = submit(builder)
    print("go submitted:", go_node.pk)
    wait_for_node_finished(go_node)
    print("go finished: exit", go_node.exit_status,
          "te", go_node.outputs.results["total_energy"],
          "converged", go_node.outputs.results["convervence"])
    potential = go_node.outputs.potential

    # ---- dos
    dos_param = make_go_param(orm.Str("2nd"), orm.Float(2.0))
    builder = common_builder(code, "akaikkr.dos", structure, dos_param, magtype, potential, ncores=args.ncores)
    dos_node = submit(builder)
    print("dos submitted:", dos_node.pk)

    # ---- spc31 (needs the calculated StructureData for the k-path)
    spc_param = make_go_param(orm.Str("2nd"), orm.Float(2.0))
    builder = common_builder(code, "akaikkr.spc", structure, spc_param, magtype, potential, ncores=args.ncores)
    builder.structure_data = go_node.outputs.structure
    spc_node = submit(builder)
    print("spc submitted:", spc_node.pk)

    wait_for_node_finished(dos_node)
    print("dos finished: exit", dos_node.exit_status, "outputs", list(dos_node.outputs))
    if dos_node.exit_status == 0:
        dos = dos_node.outputs.dos
        print("dos arrays", dos.get_arraynames(), dos.get_array("dos").shape)

    wait_for_node_finished(spc_node)
    print("spc finished: exit", spc_node.exit_status, "outputs", list(spc_node.outputs))
    if spc_node.exit_status == 0:
        print("klabel", spc_node.outputs.klabel.get_dict())

    print("PKS", go_node.pk, dos_node.pk, spc_node.pk)


if __name__ == "__main__":
    main()
