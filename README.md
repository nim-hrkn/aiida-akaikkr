# aiida-akaikkr

[AiiDA](https://www.aiida.net/) plugin for [AkaiKKR](http://kkr.issp.u-tokyo.ac.jp/) (`specx`).
It submits AkaiKKR runs as AiiDA CalcJobs, keeps every input and output as a node with full provenance,
and parses the output card into AiiDA data types with [pyakaikkr](https://github.com/nim-hrkn/AkaiKKRPythonUtil).

## What it does

| entry point | AkaiKKR mode | extra inputs | outputs (besides `results` and `structure`) |
|---|---|---|---|
| `akaikkr.go` | `go` (SCF) | | `potential` |
| `akaikkr.fsm` | `fsm` (fixed spin moment) | `fspin` | `potential` |
| `akaikkr.dos` | `dos` | | `dos`, `pdos` |
| `akaikkr.jij` | `j3.0` (real-space J_ij) | | `Jij`, `Tc` |
| `akaikkr.tc` | `tc` (k-space Tc) | | `Tc` |
| `akaikkr.spc` | `spc31` (A(w,k)) | `structure_data`, `nk` | `Awk_up`, `Awk_dn`, `klabel` |
| `akaikkr.cnd` | `cnd` (conductivity, akaikkr_cnd build) | | `resistivity`, `conductivity` |

`results` is a `Dict` with total energy, moments, charges, Fermi level, convergence history, core levels and so on.
`structure` is a `StructureData` of the cell actually used; CPA partial occupancies are kept as kinds with weights.

Inputs common to all CalcJobs: `code`, `structure` (AkaiKKR-format `Dict`), `parameters` (`Dict`), `magtype` (`Str`),
`displc` (`Bool`), `potential` (`Str` path, `SinglefileData` or `RemoteData`), `retrieve_potential` (`Bool`).

Verified on 2026-09-24 against the AkaiKKRPythonUtil test references: 13 materials
(pure metals, CPA alloys, a high-entropy alloy, a semiconductor with empty spheres, SmCo5 with open-core 4f,
Fe in the local-moment-disorder state) through go / fsm / tc / j3.0 / dos / spc31 with the standard build,
and 12 materials plus cnd with the akaikkr_cnd build. All 128 CalcJobs finished with exit code 0 and total energies,
moments, Curie temperatures and resistivities agree with the references (see `docs/known_issues.md` for the few
last-digit differences).

## Requirements

- Python >= 3.9, AiiDA >= 2 (tested with 2.9.2)
- `pyakaikkr` and `akaikkr_testscript` from AkaiKKRPythonUtil (`pip install -e library/PyAkaiKKR[ase] library/AkaiKKRTestScript`)
- pymatgen, numpy, ase (pulled in by the above)
- an AkaiKKR `specx` binary registered as an AiiDA code

## Install

```bash
pip install -e .
verdi plugin list aiida.calculations | grep akaikkr    # akaikkr.basic/go/fsm/dos/jij/tc/spc/cnd
verdi daemon restart                                   # workers must reload the entry points
```

## Quick start

```python
from aiida import load_profile, orm
from aiida.engine import submit
from aiida.plugins import CalculationFactory
from pyakaikkr import AkaikkrJob
load_profile()

code = orm.load_code("specx-akaikkr@mygardenx2-slurm")
structure = orm.Dict(dict={           # AkaiKKR structure block, e.g. from akaikkr_testscript.get_kkr_struc_from_cif
    "brvtyp": "fcc", "a": 6.8314, "c/a": 1.0, "b/a": 1.0, "alpha": 90, "beta": 90, "gamma": 90,
    "ntyp": 1, "type": ["Cu"], "ncmp": [1], "rmt": [0.0], "field": [0.0], "mxl": [2],
    "anclr": [[29]], "conc": [[100.0]], "natm": 1, "atmicx": [["0.0a", "0.0b", "0.0c", "Cu"]]})
params = dict(AkaikkrJob("dummy").default); params["record"] = "init"

builder = CalculationFactory("akaikkr.go").get_builder()
builder.code = code
builder.structure = structure
builder.parameters = orm.Dict(dict=params)
builder.magtype = orm.Str("nmag")
builder.displc = orm.Bool(False)
builder.metadata.options.resources = {"num_machines": 1, "num_mpiprocs_per_machine": 1, "num_cores_per_mpiproc": 8}
go = submit(builder)
# later: go.outputs.results["total_energy"], go.outputs.potential -> input `potential` of dos / spc / tc / j3.0
```

Ready-made scripts in `example/`:

- `run_cu_go_dos_spc.py` – Cu go → dos → spc31 from a CIF (stored as `SinglefileData`).
- `run_examples.py` – the whole AkaiKKRPythonUtil test set (`--displc` for the akaikkr_cnd build, adds `cnd`).
- `compare_reference.py` – compare a result JSON with `tests/*/reference/ifort.json`.
- `plot_results.py` – DOS / PDOS / A(w,k) figures from finished nodes (`example/figures/`).

## Documentation

`docs/README.md` (Japanese) links to:

- `docs/installation.md` – AiiDA, RabbitMQ from conda-forge without sudo, profile, SLURM computer, specx codes with the ifort runtime.
- `docs/calcjobs.md` – ports, parser outputs, exit codes, k-path generation, changes since the 2022 version.
- `docs/examples.md` – the example scripts and provenance graphs.
- `docs/known_issues.md` – reference comparison, parser limits, harmless warnings.

## Notes

- `specx` is OpenMP-parallel: leave the computer's mpirun command empty and set `num_cores_per_mpiproc`; the code's prepend text exports `OMP_NUM_THREADS`.
- The akaikkr_cnd build refuses input cards without `displc` lines; set `displc=True` (the CalcJob adds zero displacements when the structure has none).
- The pymatgen → ASE conversion does not accept partial occupancies, so the parser builds `StructureData` directly from pymatgen. Empty-sphere sites (Z = 0) are dropped from the `structure` output because AiiDA has no element for them.

## License

Apache-2.0 (see `LICENSE`).
