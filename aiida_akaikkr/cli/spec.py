"""The single table of CLI subcommands.

Read by `cli/main.py` (to build argparse and dispatch) and by `mcp/server.py`
(to build the allow-list and the argv of each tool). No aiida import.

Each entry: kind (READ / SUBMIT / CONTROL), implementation ("module:function", imported lazily),
help, and the options: name -> (type, required, help). Option names use underscores here and
become `--with-dashes` on the command line.
"""
READ, SUBMIT, CONTROL = "read", "submit", "control"

_PK = ("int", True, "pk of the node")
_CODE = ("str", False, "code label, e.g. specx-akaikkr@mygardenx2-slurm (default: AKAIKKR_CODE env)")
_NCORES = ("int", False, "OpenMP threads (num_cores_per_mpiproc), default 8")
_WALLCLOCK = ("int", False, "max wallclock seconds, default 7200")
_DISPLC = ("bool", False, "add displc (required by the akaikkr_cnd build)")

SUBCOMMANDS = {
    # ---- read
    "status": dict(kind=READ, impl="aiida_akaikkr.query.nodes:status",
                   help="profile, storage, broker, daemon and codes", options={}),
    "codes": dict(kind=READ, impl="aiida_akaikkr.query.nodes:codes",
                  help="registered specx codes", options={}),
    "computer-test": dict(kind=READ, impl="aiida_akaikkr.query.nodes:computer_test",
                          help="open the transport, count scheduler jobs, write a temporary file",
                          options={"computer": ("str", True, "computer label")}),
    "daemon-status": dict(kind=READ, impl="aiida_akaikkr.query.nodes:daemon_status",
                          help="daemon workers", options={}),
    "presets": dict(kind=READ, impl="aiida_akaikkr.query.nodes:presets",
                    help="material presets of the AkaiKKR test set", options={}),
    "process": dict(kind=READ, impl="aiida_akaikkr.query.nodes:process",
                    help="state, exit code, scheduler state, in/outputs and last reports of a process",
                    options={"pk": _PK}),
    "list": dict(kind=READ, impl="aiida_akaikkr.query.nodes:list_processes",
                 help="recent akaikkr processes",
                 options={"label_prefix": ("str", False, "only labels starting with this"),
                          "days": ("int", False, "created within the last N days (default 7)"),
                          "state": ("str", False, "created|waiting|running|finished|excepted|killed"),
                          "limit": ("int", False, "max rows (default 50)")}),
    "wait": dict(kind=READ, impl="aiida_akaikkr.query.nodes:wait",
                 help="wait (at most 45 s) for a process to terminate",
                 options={"pk": _PK, "wait_seconds": ("int", False, "seconds to wait, max 45 (default 30)")}),
    "results": dict(kind=READ, impl="aiida_akaikkr.query.nodes:results",
                    help="summary of the results of a specx CalcJob or of a chain WorkChain",
                    options={"pk": _PK}),
    "dos": dict(kind=READ, impl="aiida_akaikkr.query.nodes:dos",
                help="DOS / PDOS arrays of a dos CalcJob (thinned)",
                options={"pk": _PK, "emin": ("float", False, "lower energy bound (Ry, relative to E_F)"),
                         "emax": ("float", False, "upper energy bound"),
                         "max_points": ("int", False, "max energy points returned (default 200)")}),
    "awk": dict(kind=READ, impl="aiida_akaikkr.query.nodes:awk",
                help="k-labels and shape of A(w,k) of a spc CalcJob",
                options={"pk": _PK, "spin": ("str", False, "up|dn (default up)")}),
    "jij": dict(kind=READ, impl="aiida_akaikkr.query.nodes:jij",
                help="J_ij table and Tc of a jij CalcJob",
                options={"pk": _PK, "max_rows": ("int", False, "max rows (default 50)")}),
    "workdir": dict(kind=READ, impl="aiida_akaikkr.query.nodes:workdir",
                    help="remote work directory and the head of go.in / go.out",
                    options={"pk": _PK, "lines": ("int", False, "lines of go.out head/tail (default 8)")}),
    "plot": dict(kind=READ, impl="aiida_akaikkr.plot:plot_cli",
                 help="write DOS / PDOS / A(w,k) PNG files",
                 options={"dos_pk": ("int", False, "pk of a dos CalcJob"),
                          "spc_pk": ("int", False, "pk of a spc CalcJob"),
                          "outdir": ("str", False, "output directory (default ~/aiida_work/figures/<pk>)"),
                          "prefix": ("str", False, "file prefix (default: process label)")}),
    "compare": dict(kind=READ, impl="aiida_akaikkr.query.compare:compare_cli",
                    help="compare results with an AkaiKKRPythonUtil reference JSON",
                    options={"pks": ("str", False, "comma separated pks of CalcJobs or chain WorkChains"),
                             "result_json": ("str", False, "result JSON written by run_examples.py"),
                             "reference_json": ("str", True, "tests/<set>/reference/ifort.json"),
                             "material": ("str", False, "rename labels to <material>_<mode> (for unlabeled or chain jobs)")}),
    "provenance": dict(kind=READ, impl="aiida_akaikkr.query.nodes:provenance",
                       help="draw the provenance graph with graphviz",
                       options={"pk": _PK, "outdir": ("str", False, "output directory"),
                                "ancestor_depth": ("int", False, "default 5"),
                                "descendant_depth": ("int", False, "default 3"),
                                "fmt": ("str", False, "png|pdf|svg (default png)")}),
    # ---- submit
    "structure": dict(kind=SUBMIT, impl="aiida_akaikkr.cli.steps:structure_from_cif",
                      help="make the AkaiKKR structure/common parameters from a CIF (runs specx in geometry mode)",
                      options={"cif_path": ("str", False, "CIF file (or use --preset)"),
                               "preset": ("str", False, "preset name; uses its CIF and common-param function"),
                               "code": _CODE, "displc": _DISPLC,
                               "magtype": ("str", False, "nmag|mag|lmd (default: preset value or nmag)")}),
    "submit-go": dict(kind=SUBMIT, impl="aiida_akaikkr.cli.steps:submit_go",
                      help="submit a go (SCF) CalcJob",
                      options={"structure_pk": ("int", True, "pk of the common-parameter Dict (from `structure`)"),
                               "code": _CODE, "displc": _DISPLC, "ncores": _NCORES, "wallclock": _WALLCLOCK,
                               "label": ("str", False, "process label"),
                               "parameters": ("str", False, "JSON dict of AkaiKKR parameters to override")}),
    "submit-followup": dict(kind=SUBMIT, impl="aiida_akaikkr.cli.steps:submit_followup",
                            help="submit dos/spc/tc/jij/fsm/cnd from a finished go",
                            options={"go_pk": ("int", True, "pk of the finished go CalcJob"),
                                     "mode": ("str", True, "dos|spc|tc|jij|fsm|cnd"),
                                     "fspin": ("float", False, "fixed spin moment (fsm)"),
                                     "from_potential": ("bool", False, "fsm: restart from the go potential (record=2nd)"),
                                     "spc_structure_pk": ("int", False, "spc: StructureData for the k-path (default: go structure)"),
                                     "ncores": _NCORES, "wallclock": _WALLCLOCK,
                                     "parameters": ("str", False, "JSON dict of parameter overrides")}),
    "submit-chain": dict(kind=SUBMIT, impl="aiida_akaikkr.cli.steps:submit_chain",
                         help="submit go followed by the given modes as one WorkChain",
                         options={"structure_pk": ("int", False, "pk of the common-parameter Dict"),
                                  "preset": ("str", False, "preset name (makes the structure if structure_pk is not given)"),
                                  "cif_path": ("str", False, "CIF file (with --preset for the parameters)"),
                                  "modes": ("str", False, "comma separated modes (default: preset modes)"),
                                  "fspin": ("float", False, "fsm moment (default: preset)"),
                                  "code": _CODE, "displc": _DISPLC, "ncores": _NCORES, "wallclock": _WALLCLOCK,
                                  "label": ("str", False, "label prefix (default: preset name)")}),
    # ---- control
    "daemon-start": dict(kind=CONTROL, impl="aiida_akaikkr.cli.control:daemon_start",
                         help="start the daemon", options={"workers": ("int", False, "number of workers (default 1)")}),
    "daemon-stop": dict(kind=CONTROL, impl="aiida_akaikkr.cli.control:daemon_stop",
                        help="stop the daemon", options={}),
    "kill": dict(kind=CONTROL, impl="aiida_akaikkr.cli.control:kill",
                 help="kill a running process", options={"pk": _PK}),
}

KINDS = (READ, SUBMIT, CONTROL)


def subcommands_of_kind(kind: str) -> list:
    return [name for name, s in SUBCOMMANDS.items() if s["kind"] == kind]


def option_flag(name: str) -> str:
    return "--" + name.replace("_", "-")
