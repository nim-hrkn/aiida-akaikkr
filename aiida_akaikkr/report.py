"""HTML report of an aiida-akaikkr calculation (chain WorkChain, GAES WorkChain or a go CalcJob and its
follow-ups): the material is collected from the nodes and handed to pyakaikkr.report, which draws the
figures (pyakaikkr.plot, SVG + PNG) and renders the page in English or Japanese (docs/report_spec.md)."""
import os

from .plot import STYLE, contour_bottom, jij_dataframe, _component_shortnames, load_node


def _results(node):
    return node.outputs.results.get_dict() if node is not None and "results" in node.outputs else {}


def _go_and_followups(node):
    """(go CalcJob, {mode: CalcJob}) for a chain / GAES WorkChain or a go CalcJob (follow-ups through the
    potential link: every CalcJob whose `potential` input is the go's potential output)."""
    from aiida import orm

    label = node.process_label or ""
    follow = {}
    go = None
    if label == "AkaikkrChainWorkChain" or label == "AkaikkrGaesWorkChain":
        for link in node.base.links.get_outgoing(node_class=orm.CalcJobNode).all():
            calc = link.node
            mode = (calc.process_label or "").replace("specx_", "")
            if mode == "go":
                if go is None or calc.pk > go.pk:      # GAES: the last go
                    go = calc
            else:
                follow[mode] = calc
        if label == "AkaikkrGaesWorkChain":
            follow = {"dos": follow.get("dos")} if follow.get("dos") else {}
    else:
        go = node
    if go is not None and "potential" in go.outputs:
        for link in go.outputs.potential.base.links.get_outgoing(node_class=orm.CalcJobNode).all():
            calc = link.node
            mode = (calc.process_label or "").replace("specx_", "")
            if mode not in follow and calc.is_finished_ok:
                follow[mode] = calc
    return go, follow


def _structure_source(go):
    """{cif, poscar, preset, comp} from the provenance of the go's `structure` (common) input: the Dict may come
    from split_param(common, mode) <- make_common_param(func_name, code, cif, ...) or single_site_common_param(comp, ...);
    the creators are followed upwards (at most 4 steps) and the first inputs found win."""
    src = {"cif": None, "poscar": None, "preset": None, "comp": None}
    try:
        node = go.inputs.structure if "structure" in go.inputs else None
        for _ in range(4):
            creator = node.creator if node is not None else None
            if creator is None:
                break
            ins = {link.link_label: link.node for link in creator.base.links.get_incoming().all()}
            if "cif" in ins and src["cif"] is None:
                src["cif"] = getattr(ins["cif"], "filename", None)
            if "func_name" in ins and src["preset"] is None:
                name = ins["func_name"].value
                src["preset"] = name.replace("_common_param", "").strip("_") if name != "generic" else None
            if "comp" in ins and src["comp"] is None:
                src["comp"] = ins["comp"].value
            if "source" in ins and src["poscar"] is None and hasattr(ins["source"], "get_dict"):
                src["poscar"] = ins["source"].get_dict().get("poscar")
            if any(v for v in src.values()):
                break
            # go one level up through the Dict inputs of this calcfunction (split_param: `common`)
            node = next((n for label, n in ins.items() if label in ("common", "param", "structure")), None)
    except Exception:  # noqa: BLE001
        pass
    return src


def _where(calc):
    try:
        wd = calc.get_remote_workdir() or ""
    except Exception:  # noqa: BLE001
        wd = ""
    comp = calc.computer.label if calc.computer is not None else ""
    return "{}  {}".format(comp, wd).strip()


def collect_report_data(pk, dos_pk=None, spc_pk=None, jij_pk=None, cnd_pk=None, gaes_pk=None, lang="en", symprec=1e-3):
    """ReportData of the calculation `pk` (chain / GAES WorkChain or go CalcJob); explicit *_pk override the
    follow-ups found through the potential link."""
    import numpy as np
    from pyakaikkr import report as R

    node = load_node(pk)
    go, follow = _go_and_followups(node)
    if go is None:
        raise ValueError(f"Node<{pk}> has no go calculation")
    for mode, p in (("dos", dos_pk), ("spc", spc_pk), ("jij", jij_pk), ("cnd", cnd_pk)):
        if p:
            follow[mode] = load_node(p)
    gaes = None
    if gaes_pk:
        gaes = load_node(gaes_pk)
    elif node.process_label == "AkaikkrGaesWorkChain":
        gaes = node
    res = _results(go)
    data = R.ReportData(title=node.label or go.label or f"pk{node.pk}")
    # structure
    if "structure" in go.outputs:
        struc = go.outputs.structure.get_pymatgen()
        data.formula, data.formula_full = R.formula_of(struc)
        data.spacegroup = R.symmetry_of(struc, symprec)
    data.structure_source = _structure_source(go)
    sp = res.get("struc_param") or {}
    a = float(sp.get("a") or res.get("lattice_constant") or 0)
    data.lattice = {"brvtyp": sp.get("brvtyp"), "a_bohr": a, "a_angstrom": a * R.BOHR_TO_ANG if a else None}
    for k in ("c/a", "b/a", "alpha", "beta", "gamma"):
        if sp.get(k):
            data.lattice[k] = sp[k]
    data.natm = len(res.get("atom_names") or []) or None
    # calculation
    code = go.inputs.code if "code" in go.inputs else None
    params = go.inputs.parameters.get_dict() if "parameters" in go.inputs else {}
    data.calc = {"code": code.full_label if code is not None else None, "magtyp": res.get("magtyp"),
                 "sdftyp": params.get("sdftyp"), "reltyp": params.get("reltyp"), "ewidth": res.get("ewidth"),
                 "edelt": res.get("edelt"), "bzqlty": params.get("bzqlty"), "maxitr": params.get("maxitr"), "pmix": params.get("pmix")}
    data.calc = {k: v for k, v in data.calc.items() if v is not None}
    rms = res.get("rms_error") or []
    data.scf = {"converged": res.get("convervence"), "n_iter": len(rms) or None, "rms_last": rms[-1] if rms else None,
                "fermi_level": res.get("fermi_level"), "total_energy_Ry": res.get("total_energy"), "total_moment": res.get("total_moment")}
    tos = res.get("type_of_site") or []
    data.components = R.components_from_outputs(tos, res.get("local_moment"), None, res.get("type_charge"))
    data.provenance.append({"step": "go", "pk": go.pk, "where": _where(go)})
    # dos
    dos = follow.get("dos")
    if dos is not None and "dos" in dos.outputs:
        ebtm, _label = contour_bottom(dos)
        energy = dos.outputs.dos.get_array("energy")
        R.add_dos_figure(data, energy, dos.outputs.dos.get_array("dos"), ewidth_go=-ebtm, lang=lang, style=STYLE)
        if "pdos" in dos.outputs:
            pdos = dos.outputs.pdos.get_array("pdos")
            from pyakaikkr.plot import component_names
            R.add_pdos_figure(data, dos.outputs.pdos.get_array("energy"), pdos, component_names(_results(dos).get("type_of_site") or tos),
                              ewidth_go=-ebtm, lang=lang, style=STYLE)
        data.provenance.append({"step": "dos", "pk": dos.pk, "where": _where(dos)})
    # spc
    spc = follow.get("spc")
    if spc is not None and "klabel" in spc.outputs:
        from pyakaikkr import AwkReader
        klabel = ["$" + str(list(v.keys())[0]) + "$" for path in spc.outputs.klabel.get_dict()["kpath"] for v in path]
        awk = {}
        for port, spin in (("Awk_up", "up"), ("Awk_dn", "dn")):
            if port in spc.outputs:
                f = spc.outputs[port]
                try:
                    with f.open(f.filename) as handle:
                        awk[spin] = AwkReader(handle)
                except Exception:  # noqa: BLE001
                    pass
        if awk:
            first = next(iter(awk.values()))
            R.add_awk_figures(data, {s: r.Awk for s, r in awk.items()}, first.kdist, first.energy, first.kcrt, klabel, lang=lang, style=STYLE)
        data.provenance.append({"step": "spc", "pk": spc.pk, "where": _where(spc)})
    # jij
    jij = follow.get("jij")
    if jij is not None and "Jij" in jij.outputs:
        data.tc = jij.outputs.Tc.value if "Tc" in jij.outputs else None
        R.add_jij_figures(data, jij_dataframe(jij), _component_shortnames(jij), tc=data.tc, lang=lang, style=STYLE)
        data.provenance.append({"step": "jij", "pk": jij.pk, "where": _where(jij)})
    tc = follow.get("tc")
    if tc is not None and data.tc is None and "Tc" in tc.outputs:
        data.tc = tc.outputs.Tc.value
    # cnd
    cnd = follow.get("cnd")
    if cnd is not None and "resistivity" in cnd.outputs:
        data.cnd = {"resistivity": cnd.outputs.resistivity.value,
                    "conductivity": cnd.outputs.conductivity.get_list() if "conductivity" in cnd.outputs else None}
        data.provenance.append({"step": "cnd", "pk": cnd.pk, "where": _where(cnd)})
    # gaes
    if gaes is not None and "history" in gaes.outputs:
        hist = gaes.outputs.history.get_list()
        gp = gaes.outputs.parameters.get_dict()
        data.gaes = {"status": gaes.outputs.status.value if "status" in gaes.outputs else None,
                     "ewidth": gaes.outputs.ewidth.value if "ewidth" in gaes.outputs else None,
                     "tried": [h["ewidth"] for h in hist], "gap": (hist[-1].get("gap_used") if hist else None), "history": hist}
        if hist:
            h = hist[-1]
            dnode = load_node(h["dos_pk"])
            energy = dnode.outputs.dos.get_array("energy")
            dosarr = np.asarray(dnode.outputs.dos.get_array("dos"), dtype=float)
            lo, hi = h.get("orbital_bounds") or (gp.get("min_ewidth") or 1.0, gp.get("max_ewidth") or 2.0)
            R.add_gaes_figure(data, energy, dosarr, h["ewidth"], coarse=h.get("coarse_regions", []), fine=h.get("fine_regions", []),
                              bounds=(lo, hi), levels=h.get("orbital_levels", {}), natm=h.get("natm"), dosth=gp.get("dosth"),
                              dosth2=gp.get("dosth2"), final=data.gaes["ewidth"], lang=lang, style=STYLE)
        data.provenance.append({"step": "gaes", "pk": gaes.pk, "where": ""})
    return data


def write_report(pk, outdir=None, prefix=None, lang="en", embed="svg", dos_pk=None, spc_pk=None, jij_pk=None, cnd_pk=None,
                 gaes_pk=None, figures=True):
    """HTML report file of `pk`; returns {"html": path, "figures": [paths], "summary": {...}}."""
    from pyakaikkr import report as R

    node = load_node(pk)
    outdir = outdir or os.path.join(os.path.expanduser("~"), "aiida_work", "figures", str(node.pk))
    os.makedirs(outdir, exist_ok=True)
    prefix = prefix or (node.label.rsplit("_", 1)[0] if node.label else f"pk{node.pk}")
    data = collect_report_data(pk, dos_pk=dos_pk, spc_pk=spc_pk, jij_pk=jij_pk, cnd_pk=cnd_pk, gaes_pk=gaes_pk, lang=lang)
    path = os.path.join(outdir, f"{prefix}_report_{lang}.html")
    figure_dir = os.path.join(outdir, f"{prefix}_report_figures") if figures else None
    R.write_report(data, path, lang=lang, embed=embed, figure_dir=figure_dir)
    written = []
    if figure_dir:
        written = sorted(os.path.join(figure_dir, f) for f in os.listdir(figure_dir))
    return {"html": path, "figures": written, "summary": data.summary()}


def report_cli(pk, outdir=None, prefix=None, lang="en", embed="svg", dos_pk=None, spc_pk=None, jij_pk=None, cnd_pk=None, gaes_pk=None):
    return write_report(int(pk), outdir=outdir, prefix=prefix, lang=lang, embed=embed,
                        dos_pk=dos_pk, spc_pk=spc_pk, jij_pk=jij_pk, cnd_pk=cnd_pk, gaes_pk=gaes_pk)
