"""Read-only queries over the AiiDA database (aiida imported lazily)."""
import datetime
import os
import time

from ..presets import CND_MATERIALS, FSM_FROM_GO_POTENTIAL, MATERIALS, MODES, SPC_STRUCTURE_FROM, structure_dir

CHAIN_LABEL = "AkaikkrChainWorkChain"


def input_nodes(node) -> dict:
    """link label -> node of the inputs (INPUT_CALC / INPUT_WORK)."""
    from aiida.common.links import LinkType

    links = node.base.links.get_incoming(link_type=(LinkType.INPUT_CALC, LinkType.INPUT_WORK)).all()
    return {link.link_label: link.node for link in links}


def output_nodes(node) -> dict:
    """link label -> node of the outputs (CREATE / RETURN)."""
    from aiida.common.links import LinkType

    links = node.base.links.get_outgoing(link_type=(LinkType.CREATE, LinkType.RETURN)).all()
    return {link.link_label: link.node for link in links}


def _node(pk):
    from aiida import orm
    from aiida.common.exceptions import NotExistent

    try:
        return orm.load_node(int(pk))
    except NotExistent as exc:
        raise ValueError(f"no node with pk {pk}") from exc


def _is_specx(node):
    return (node.process_label or "").startswith("specx_")


def _is_chain(node):
    return node.process_label == CHAIN_LABEL


def _results_summary(node):
    """summary of a specx CalcJob's outputs."""
    d = {"pk": node.pk, "label": node.label, "process_label": node.process_label,
         "state": node.process_state.value if node.process_state else None, "exit_status": node.exit_status,
         "outputs": sorted(output_nodes(node))}
    if "results" in node.outputs:
        r = node.outputs.results
        d.update({"go": r.get("go"), "total_energy": r.get("total_energy"), "total_moment": r.get("total_moment"),
                  "local_moment": r.get("local_moment"), "converged": r.get("convervence"),
                  "fermi_level": r.get("fermi_level"),
                  "type_of_site": [{"type": t.get("type"), "components": t.get("comp_shortname")}
                                   for t in (r.get("type_of_site") or [])]})
    if "Tc" in node.outputs:
        d["Tc"] = node.outputs.Tc.value
    if "resistivity" in node.outputs:
        d["resistivity"] = node.outputs.resistivity.value
        d["conductivity"] = node.outputs.conductivity.get_list()
    return d


# ---------------------------------------------------------------- environment
def status():
    from aiida import orm
    from aiida.manage import get_manager

    profile = get_manager().get_profile()
    info = {"profile": profile.name, "storage_backend": profile.storage_backend,
            "broker_backend": profile.process_control_backend}
    try:
        from .. import cli  # noqa: F401
        from ..cli.control import daemon_status

        info["daemon"] = daemon_status()
    except Exception as exc:  # noqa: BLE001
        info["daemon"] = {"error": str(exc)}
    info["codes"] = [c["label"] for c in codes()["codes"]]
    info["computers"] = [c.label for c in orm.Computer.collection.all()]
    info["default_code_env"] = os.environ.get("AKAIKKR_CODE")
    return info


def codes():
    from aiida import orm

    rows = []
    for code in orm.QueryBuilder().append(orm.InstalledCode).all(flat=True):
        rows.append({"label": code.full_label, "pk": code.pk, "computer": code.computer.label,
                     "executable": str(code.filepath_executable),
                     "default_calc_job_plugin": code.default_calc_job_plugin,
                     "has_prepend_text": bool(code.prepend_text.strip())})
    return {"codes": rows}


def computer_test(computer):
    from aiida import orm

    comp = orm.load_computer(computer)
    out = {"computer": comp.label, "scheduler": comp.scheduler_type, "transport": comp.transport_type, "checks": {}}
    with comp.get_transport() as transport:
        out["checks"]["transport_open"] = True
        scheduler = comp.get_scheduler()
        scheduler.set_transport(transport)
        jobs = scheduler.get_jobs(as_dict=True)
        out["checks"]["scheduler_jobs"] = len(jobs)
        rc, stdout, stderr = transport.exec_command_wait("echo akaikkr-ok")
        out["checks"]["exec_command"] = stdout.strip() == "akaikkr-ok"
        out["checks"]["remote_user"] = transport.whoami()
    out["ok_all"] = all(v is True or isinstance(v, (int, str)) for v in out["checks"].values())
    return out


def daemon_status():
    from ..cli.control import daemon_status as _ds

    return _ds()


def presets():
    rows = []
    for name, m in MATERIALS.items():
        rows.append({"name": name, "cif": m["cif"], "modes": m["modes"], "fspin": m["fspin"],
                     "fsm_from_potential": name in FSM_FROM_GO_POTENTIAL, "cnd_with_displc": name in CND_MATERIALS,
                     "spc_structure_from": SPC_STRUCTURE_FROM.get(name)})
    return {"structure_dir": structure_dir(), "modes": {m: {"entry_point": e, "go": g} for m, (e, g) in MODES.items()},
            "presets": rows}


# ---------------------------------------------------------------- processes
def process(pk):
    from aiida import orm

    node = _node(pk)
    if not isinstance(node, orm.ProcessNode):
        raise ValueError(f"Node<{node.pk}> is a {node.__class__.__name__}, not a process")
    d = {"pk": node.pk, "label": node.label, "process_label": node.process_label,
         "state": node.process_state.value if node.process_state else None,
         "exit_status": node.exit_status, "exit_message": node.exit_message,
         "ctime": node.ctime.isoformat(timespec="seconds"),
         "inputs": {k: v.pk for k, v in input_nodes(node).items()}, "outputs": sorted(output_nodes(node))}
    if isinstance(node, orm.CalcJobNode):
        d["scheduler_state"] = str(node.get_scheduler_state()) if node.get_scheduler_state() else None
        d["job_id"] = node.get_job_id()
        d["remote_workdir"] = node.get_remote_workdir()
    logs = orm.Log.collection.get_logs_for(node)
    d["report_tail"] = [f"{log.levelname}: {log.message}" for log in logs[-3:]]
    if isinstance(node, orm.CalcJobNode) and node.exit_status == 321 and "retrieved" in node.outputs:
        rep = node.outputs.retrieved.base.repository
        name = node.get_option("output_filename")
        if name in rep.list_object_names():
            d["stdout_head"] = rep.get_object_content(name).splitlines()[:6]
            d["hint"] = "exit 321 (no pot.dat) usually means specx rejected the input card; see stdout_head"
    if _is_chain(node):
        d["children"] = [{"pk": c.pk, "label": c.label, "state": c.process_state.value, "exit_status": c.exit_status}
                         for c in node.called]
    return d


def list_processes(label_prefix=None, days=None, state=None, limit=None):
    from aiida import orm

    since = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(days=days or 7)
    filters = {"ctime": {">": since},
               "attributes.process_label": {"or": [{"like": "specx_%"}, {"==": CHAIN_LABEL}]}}
    if label_prefix:
        filters["label"] = {"like": f"{label_prefix}%"}
    if state:
        filters["attributes.process_state"] = state
    qb = orm.QueryBuilder().append(orm.ProcessNode, filters=filters, tag="p")
    qb.order_by({"p": {"ctime": "desc"}}).limit(limit or 50)
    rows = [{"pk": n.pk, "label": n.label, "process_label": n.process_label,
             "state": n.process_state.value if n.process_state else None, "exit_status": n.exit_status,
             "ctime": n.ctime.isoformat(timespec="seconds")} for n in qb.all(flat=True)]
    return {"count": len(rows), "processes": rows}


def wait(pk, wait_seconds=None):
    node = _node(pk)
    seconds = min(int(wait_seconds or 30), 45)
    t0 = time.time()
    while not node.is_terminated and time.time() - t0 < seconds:
        time.sleep(2)
    return {"pk": node.pk, "terminated": node.is_terminated, "state": node.process_state.value,
            "exit_status": node.exit_status, "waited_seconds": round(time.time() - t0, 1)}


def results(pk):
    node = _node(pk)
    if _is_chain(node):
        out = {"pk": node.pk, "label": node.label, "process_label": node.process_label,
               "state": node.process_state.value, "exit_status": node.exit_status, "modes": {}}
        for child in node.called:
            if _is_specx(child):
                mode = child.label.rsplit("_", 1)[-1] if child.label else child.process_label
                out["modes"][mode] = _results_summary(child)
        return out
    if not _is_specx(node):
        raise ValueError(f"Node<{node.pk}> is not a specx CalcJob or a chain (process_label={node.process_label})")
    return _results_summary(node)


# ---------------------------------------------------------------- arrays
def _thin(n, max_points):
    import numpy as np

    if n <= max_points:
        return np.arange(n)
    return np.unique(np.linspace(0, n - 1, max_points).astype(int))


def dos(pk, emin=None, emax=None, max_points=None):
    import numpy as np

    node = _node(pk)
    if "dos" not in node.outputs:
        raise ValueError(f"Node<{node.pk}> has no `dos` output (is it a dos CalcJob?)")
    energy = node.outputs.dos.get_array("energy")
    dosarr = node.outputs.dos.get_array("dos")
    mask = np.ones_like(energy, dtype=bool)
    if emin is not None:
        mask &= energy >= emin
    if emax is not None:
        mask &= energy <= emax
    idx = np.where(mask)[0]
    idx = idx[_thin(len(idx), max_points or 200)]
    out = {"pk": node.pk, "label": node.label, "n_energy": int(len(energy)), "n_spin": int(dosarr.shape[0]),
           "energy_unit": "Ry relative to E_F", "energy": energy[idx].round(6).tolist(),
           "dos": dosarr[:, idx].round(5).tolist()}
    if "pdos" in node.outputs:
        p = node.outputs.pdos
        parr = p.get_array("pdos")
        comps = [c for t in node.outputs.results["type_of_site"] for c in t["comp_shortname"]]
        out["pdos"] = {"shape": list(parr.shape), "components": comps,
                       "nl_per_type": p.get_array("nl_per_type").tolist() if "nl_per_type" in p.get_arraynames() else None,
                       "note": "axes: spin, component, energy, l (NaN padded); use `plot` for figures"}
    return out


def awk(pk, spin=None):
    import io

    from pyakaikkr import AwkReader

    node = _node(pk)
    port = f"Awk_{spin or 'up'}"
    if port not in node.outputs:
        raise ValueError(f"Node<{node.pk}> has no `{port}` output; available: {sorted(output_nodes(node))}")
    f = node.outputs[port]
    with f.open(f.filename) as handle:
        reader = AwkReader(io.StringIO(handle.read()) if isinstance(handle.read(0), str) else handle)
    klabel = [str(list(v.keys())[0]) for path in node.outputs.klabel.get_dict()["kpath"] for v in path]
    return {"pk": node.pk, "label": node.label, "spin": spin or "up", "klabel": klabel,
            "n_k": int(len(reader.kdist)), "n_energy": int(len(reader.energy)),
            "energy_range_Ry": [float(reader.energy.min()), float(reader.energy.max())],
            "k_nodes": [float(x) for x in reader.kdist[reader.kcrt]], "Awk_max": float(reader.Awk.max()),
            "note": "the A(w,k) matrix is not returned; use `plot --spc-pk`"}


def jij(pk, max_rows=None):
    node = _node(pk)
    if "Jij" not in node.outputs:
        raise ValueError(f"Node<{node.pk}> has no `Jij` output (is it a jij CalcJob?)")
    table = node.outputs.Jij.get_dict()
    cols = list(table)
    n = len(table[cols[0]]) if cols else 0
    m = min(n, max_rows or 50)
    rows = [{c: table[c][i] for c in cols} for i in range(m)]
    return {"pk": node.pk, "label": node.label, "Tc": node.outputs.Tc.value if "Tc" in node.outputs else None,
            "n_rows": n, "rows": rows}


def workdir(pk, lines=None):
    node = _node(pk)
    n = lines or 8
    d = {"pk": node.pk, "label": node.label, "remote_workdir": node.get_remote_workdir()}
    if "retrieved" in node.outputs:
        rep = node.outputs.retrieved.base.repository
        d["retrieved_files"] = rep.list_object_names()
        for opt, key in (("input_filename", "stdin_head"), ("output_filename", "stdout_head")):
            name = node.get_option(opt)
            if name in d["retrieved_files"]:
                content = rep.get_object_content(name).splitlines()
                d[key] = content[:n]
                if key == "stdout_head":
                    d["stdout_tail"] = content[-n:]
    return d


def provenance(pk, outdir=None, ancestor_depth=None, descendant_depth=None, fmt=None):
    from aiida.tools.visualization.graph import Graph

    node = _node(pk)
    outdir = outdir or os.path.join(os.path.expanduser("~"), "aiida_work", "figures", str(node.pk))
    os.makedirs(outdir, exist_ok=True)
    graph = Graph(graph_attr={"rankdir": "TB"})
    graph.recurse_ancestors(node, depth=ancestor_depth or 5, annotate_links="both", include_process_outputs=True)
    graph.recurse_descendants(node, depth=descendant_depth or 3, annotate_links="both", include_process_inputs=True)
    base = os.path.join(outdir, f"provenance_{node.pk}")
    path = graph.graphviz.render(base, format=fmt or "png", cleanup=True)
    return {"pk": node.pk, "file": path, "nodes": len(graph.nodes), "edges": len(graph.edges)}
