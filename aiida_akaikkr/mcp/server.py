"""`akaikkr-mcp`: MCP server exposing the `akaikkr-aiida` CLI as tools.

Promises (docs/mcp_design.md):
- never imports aiida or pyakaikkr; it only runs the `akaikkr-aiida` binary with `--json`;
- the only binary it can run is `akaikkr-aiida` and the only subcommands are those of cli/spec.py;
- every call is bounded by TIMEOUT (< 60 s bridge); submissions return a pk;
- submit tools appear only with --allow-submit, control tools only with --allow-control.
"""
import argparse
import json
import os
import shutil
import subprocess
import sys

from ..cli.spec import CONTROL, READ, SUBCOMMANDS, SUBMIT, option_flag

ALLOWED_BINARY_NAMES = ("akaikkr-aiida",)
ALLOWED_SUBCOMMANDS = frozenset(SUBCOMMANDS)
TIMEOUT = 55

# tool name -> subcommand
TOOLS = {
    "kkr_status": "status", "kkr_codes": "codes", "kkr_computer_test": "computer-test",
    "kkr_daemon_status": "daemon-status", "kkr_presets": "presets", "kkr_process": "process",
    "kkr_list": "list", "kkr_wait": "wait", "kkr_results": "results", "kkr_dos": "dos", "kkr_awk": "awk",
    "kkr_jij": "jij", "kkr_workdir": "workdir", "kkr_plot": "plot", "kkr_compare_reference": "compare",
    "kkr_provenance": "provenance", "kkr_report": "report",
    "kkr_structure_from_cif": "structure", "kkr_submit_go": "submit-go",
    "kkr_submit_followup": "submit-followup", "kkr_submit_chain": "submit-chain", "kkr_submit_gaes": "submit-gaes",
    "kkr_daemon_start": "daemon-start", "kkr_daemon_stop": "daemon-stop", "kkr_kill": "kill",
}

_SETTINGS = {"profile": None}


def binary() -> str:
    """path of the akaikkr-aiida binary: AKAIKKR_AIIDA_BIN, else next to this interpreter, else PATH."""
    env = os.environ.get("AKAIKKR_AIIDA_BIN")
    if env:
        path = env
    else:
        path = os.path.join(os.path.dirname(sys.executable), "akaikkr-aiida")
        if not os.path.exists(path):
            path = shutil.which("akaikkr-aiida") or path
    if os.path.basename(path) not in ALLOWED_BINARY_NAMES:
        raise RuntimeError(f"refusing to run {path!r}: only {ALLOWED_BINARY_NAMES} are allowed")
    return path


def build_argv(subcommand: str, kwargs: dict) -> list:
    """argv of the CLI call; every non-None argument of the tool is passed as an option."""
    if subcommand not in ALLOWED_SUBCOMMANDS:
        raise ValueError(f"subcommand {subcommand!r} is not allowed")
    options = SUBCOMMANDS[subcommand]["options"]
    unknown = set(kwargs) - set(options)
    if unknown:
        raise ValueError(f"{subcommand}: unknown arguments {sorted(unknown)}")
    argv = [binary(), "--json", "--caller", "mcp"]
    if _SETTINGS["profile"]:
        argv += ["--profile", _SETTINGS["profile"]]
    argv.append(subcommand)
    for name, (typ, _required, _help) in options.items():
        value = kwargs.get(name)
        if value is None:
            continue
        if typ == "bool":
            if value:
                argv.append(option_flag(name))
        else:
            argv += [option_flag(name), str(value)]
    return argv


def run(subcommand: str, **kwargs) -> dict:
    argv = build_argv(subcommand, kwargs)
    try:
        proc = subprocess.run(argv, capture_output=True, text=True, timeout=TIMEOUT)
    except subprocess.TimeoutExpired:
        return {"ok": False, "error": f"{subcommand} did not finish within {TIMEOUT} s",
                "hint": "the CLI may still be running; use kkr_list / kkr_process to check"}
    stderr_tail = proc.stderr.strip().splitlines()[-5:]
    try:
        out = json.loads(proc.stdout.strip().splitlines()[-1])
    except (ValueError, IndexError):
        return {"ok": False, "error": f"{subcommand} returned no JSON (exit {proc.returncode})",
                "stdout": proc.stdout[-2000:], "stderr_tail": stderr_tail}
    if not out.get("ok") and stderr_tail:
        out["stderr_tail"] = stderr_tail
    return out


# ---------------------------------------------------------------- tools (READ)
def kkr_status() -> dict:
    """AiiDA profile, storage, broker, daemon state, registered specx codes and computers."""
    return run("status")


def kkr_codes() -> dict:
    """Registered specx codes (label, computer, executable)."""
    return run("codes")


def kkr_computer_test(computer: str) -> dict:
    """Open the transport of a computer, count scheduler jobs, run a command (like `verdi computer test`)."""
    return run("computer-test", computer=computer)


def kkr_daemon_status() -> dict:
    """Whether the AiiDA daemon runs and with how many workers."""
    return run("daemon-status")


def kkr_presets() -> dict:
    """Material presets of the AkaiKKR test set (CIF, follow-up modes, fspin) and the mode table."""
    return run("presets")


def kkr_process(pk: int) -> dict:
    """State, exit code, scheduler state, inputs/outputs and last reports of a process; stdout head on exit 321."""
    return run("process", pk=pk)


def kkr_list(label_prefix: str | None = None, days: int | None = None, state: str | None = None,
             limit: int | None = None) -> dict:
    """Recent akaikkr processes (specx CalcJobs and chain WorkChains), newest first."""
    return run("list", label_prefix=label_prefix, days=days, state=state, limit=limit)


def kkr_wait(pk: int, wait_seconds: int | None = None) -> dict:
    """Wait at most 45 s for a process to terminate and report its state."""
    return run("wait", pk=pk, wait_seconds=wait_seconds)


def kkr_results(pk: int) -> dict:
    """Total energy, moments, convergence, Tc, resistivity of a specx CalcJob, or of every job of a chain."""
    return run("results", pk=pk)


def kkr_dos(pk: int, emin: float | None = None, emax: float | None = None, max_points: int | None = None) -> dict:
    """Energy (Ry relative to E_F) and DOS arrays of a dos CalcJob, thinned to max_points; PDOS shape and components."""
    return run("dos", pk=pk, emin=emin, emax=emax, max_points=max_points)


def kkr_awk(pk: int, spin: str | None = None) -> dict:
    """k-path labels, sizes and energy range of A(w,k) of a spc CalcJob (the matrix itself is not returned)."""
    return run("awk", pk=pk, spin=spin)


def kkr_jij(pk: int, max_rows: int | None = None) -> dict:
    """J_ij table (distance, pair, J_ij) and Tc of a jij CalcJob."""
    return run("jij", pk=pk, max_rows=max_rows)


def kkr_workdir(pk: int, lines: int | None = None) -> dict:
    """Remote work directory, retrieved files and the head/tail of go.in / go.out of a CalcJob."""
    return run("workdir", pk=pk, lines=lines)


def kkr_plot(dos_pk: int | None = None, gaes_pk: int | None = None, spc_pk: int | None = None,
             jij_pk: int | None = None, outdir: str | None = None, prefix: str | None = None) -> dict:
    """Write DOS / PDOS / A(w,k) / J_ij(R) PNG files (and <prefix>_jij.csv) for dos / spc / jij CalcJobs, or the DOS of
    every iteration of a GAES WorkChain (gaes_pk) with its gap regions; returns the file paths."""
    return run("plot", dos_pk=dos_pk, gaes_pk=gaes_pk, spc_pk=spc_pk, jij_pk=jij_pk, outdir=outdir, prefix=prefix)


def kkr_report(pk: int, lang: str | None = None, embed: str | None = None, dos_pk: int | None = None, spc_pk: int | None = None,
               jij_pk: int | None = None, cnd_pk: int | None = None, gaes_pk: int | None = None, outdir: str | None = None,
               prefix: str | None = None) -> dict:
    """Write a self-contained HTML report of a calculation: formula, structure source (CIF / POSCAR / preset), space group,
    lattice, calculation parameters, SCF results (E_F, total energy, total moment), moments and charges per component,
    figures (DOS, PDOS, A(w,k), J_ij with Tc, GAES) and cnd values. `pk` is a chain WorkChain, a GAES WorkChain or a go
    CalcJob (follow-ups are found through its potential); explicit dos_pk / spc_pk / jij_pk / cnd_pk / gaes_pk override.
    lang "en" (default) or "ja"; embed "svg" (inline, default) or "png". Returns the HTML path, the figure files and a summary."""
    return run("report", pk=pk, lang=lang, embed=embed, dos_pk=dos_pk, spc_pk=spc_pk, jij_pk=jij_pk, cnd_pk=cnd_pk, gaes_pk=gaes_pk,
               outdir=outdir, prefix=prefix)


def kkr_compare_reference(reference_json: str, pks: str | None = None, result_json: str | None = None,
                          material: str | None = None) -> dict:
    """Compare CalcJobs / chains (comma separated pks) or a result JSON with an AkaiKKRPythonUtil reference JSON.
    `material` renames the jobs to <material>_<mode> so unlabeled or chain jobs match the reference labels."""
    return run("compare", pks=pks, result_json=result_json, reference_json=reference_json, material=material)


def kkr_provenance(pk: int, outdir: str | None = None, ancestor_depth: int | None = None,
                   descendant_depth: int | None = None, fmt: str | None = None) -> dict:
    """Draw the provenance graph of a node with graphviz (png/pdf/svg); returns the file path."""
    return run("provenance", pk=pk, outdir=outdir, ancestor_depth=ancestor_depth,
               descendant_depth=descendant_depth, fmt=fmt)


# ---------------------------------------------------------------- tools (SUBMIT)
def kkr_structure_from_cif(cif_path: str | None = None, preset: str | None = None, code: str | None = None,
                           displc: bool = False, magtype: str | None = None) -> dict:
    """Make the AkaiKKR common-parameter Dict from a CIF (stored as SinglefileData); runs specx in geometry mode. Returns its pk."""
    return run("structure", cif_path=cif_path, preset=preset, code=code, displc=displc, magtype=magtype)


def kkr_submit_go(structure_pk: int, code: str | None = None, displc: bool = False, ncores: int | None = None,
                  wallclock: int | None = None, label: str | None = None, parameters: str | None = None) -> dict:
    """Submit a go (SCF) CalcJob from a common-parameter Dict pk. `parameters` is a JSON object of overrides."""
    return run("submit-go", structure_pk=structure_pk, code=code, displc=displc, ncores=ncores,
               wallclock=wallclock, label=label, parameters=parameters)


def kkr_submit_followup(go_pk: int, mode: str, fspin: float | None = None, from_potential: bool = False,
                        spc_structure_pk: int | None = None, ncores: int | None = None,
                        wallclock: int | None = None, parameters: str | None = None) -> dict:
    """Submit dos / spc / tc / jij / fsm / cnd from a finished go (its potential and structure are reused)."""
    return run("submit-followup", go_pk=go_pk, mode=mode, fspin=fspin, from_potential=from_potential,
               spc_structure_pk=spc_structure_pk, ncores=ncores, wallclock=wallclock, parameters=parameters)


def kkr_submit_chain(structure_pk: int | None = None, preset: str | None = None, cif_path: str | None = None,
                     modes: str | None = None, fspin: float | None = None, code: str | None = None,
                     displc: bool = False, ncores: int | None = None, wallclock: int | None = None,
                     label: str | None = None) -> dict:
    """Submit go followed by the given modes (comma separated) as one WorkChain; returns the WorkChain pk."""
    return run("submit-chain", structure_pk=structure_pk, preset=preset, cif_path=cif_path, modes=modes,
               fspin=fspin, code=code, displc=displc, ncores=ncores, wallclock=wallclock, label=label)


def kkr_submit_gaes(structure_pk: int | None = None, preset: str | None = None, cif_path: str | None = None,
                    comp: str | None = None, polytyp: str | None = None, lattice: str | None = None,
                    magtype: str | None = None, ewidth_init: float | None = None, method: int | None = None,
                    dosth: float | None = None, dosth2: float | None = None, min_ewidth: float | None = None,
                    max_ewidth: float | None = None, orbital: str | None = None, max_ew: int | None = None,
                    ewidth_dos: float | None = None, ref: float | None = None, parameters: str | None = None, code: str | None = None,
                    displc: bool = False, ncores: int | None = None, wallclock: int | None = None,
                    label: str | None = None) -> dict:
    """Submit a GAES WorkChain (gap-anchored ewidth search: go -> dos -> judge the DOS gap -> new ewidth, until
    E_F - ewidth lies in the gap between valence and semicore states). The system is a common-parameter Dict
    (structure_pk), a preset, a CIF, or a single-site CPA composition such as "AlSiRhBi" (comp, polytyp fcc/bcc).
    orbital = comma separated per-orbital rules such as "Rb4p=valence,Bi6s=core" (aliases occupied / unoccupied):
    the ewidth range is then derived from the core levels of each go instead of min_ewidth / max_ewidth.
    Returns the WorkChain pk; read the result with kkr_results and the figures with kkr_plot(gaes_pk=...)."""
    return run("submit-gaes", structure_pk=structure_pk, preset=preset, cif_path=cif_path, comp=comp, polytyp=polytyp,
               lattice=lattice, magtype=magtype, ewidth_init=ewidth_init, method=method, dosth=dosth, dosth2=dosth2,
               min_ewidth=min_ewidth, max_ewidth=max_ewidth, orbital=orbital, max_ew=max_ew, ewidth_dos=ewidth_dos, ref=ref,
               parameters=parameters, code=code, displc=displc, ncores=ncores, wallclock=wallclock, label=label)


# ---------------------------------------------------------------- tools (CONTROL)
def kkr_daemon_start(workers: int | None = None) -> dict:
    """Start the AiiDA daemon (logged)."""
    return run("daemon-start", workers=workers)


def kkr_daemon_stop() -> dict:
    """Stop the AiiDA daemon (logged)."""
    return run("daemon-stop")


def kkr_kill(pk: int) -> dict:
    """Kill a running process (logged)."""
    return run("kill", pk=pk)


def tool_functions(allow_submit: bool = False, allow_control: bool = False) -> dict:
    """tool name -> function, filtered by the flags."""
    allowed_kinds = {READ}
    if allow_submit:
        allowed_kinds.add(SUBMIT)
    if allow_control:
        allowed_kinds.add(CONTROL)
    module = sys.modules[__name__]
    return {name: getattr(module, name) for name, sub in TOOLS.items()
            if SUBCOMMANDS[sub]["kind"] in allowed_kinds}


def build_server(allow_submit: bool = False, allow_control: bool = False):
    from mcp.server.mcpserver import MCPServer

    flags = []
    if allow_submit:
        flags.append("submit")
    if allow_control:
        flags.append("control")
    server = MCPServer(
        name="akaikkr",
        instructions="AkaiKKR (specx) through AiiDA. Read-only tools are always available"
                     + (f"; enabled write tools: {', '.join(flags)}" if flags else "; no write tools enabled")
                     + ". Submissions are asynchronous: they return a pk, poll with kkr_process or kkr_wait.")
    for name, fn in tool_functions(allow_submit, allow_control).items():
        server.add_tool(fn, name=name, description=fn.__doc__)
    return server


def main(argv=None):
    parser = argparse.ArgumentParser(prog="akaikkr-mcp", description="MCP server for aiida-akaikkr")
    parser.add_argument("--allow-submit", action="store_true", help="expose the submit tools")
    parser.add_argument("--allow-control", action="store_true", help="expose daemon start/stop and kill")
    parser.add_argument("--profile", default=None, help="AiiDA profile passed to every CLI call")
    args = parser.parse_args(argv)
    _SETTINGS["profile"] = args.profile
    build_server(args.allow_submit, args.allow_control).run("stdio")


if __name__ == "__main__":
    main()
