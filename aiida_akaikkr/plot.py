# Copyright (c) 2022-2026 Hiori Kino.
# Distributed under the terms of the Apache License, Version 2.0.
"""DOS / PDOS / A(w,k) / J_ij / GAES figures from finished aiida-akaikkr nodes.

The drawing itself is done by ``pyakaikkr.plot`` (the array-based "plot function A" shared with
pyakaikkr's own plotters): this module only takes the arrays of the ArrayData / Dict outputs of a
node, adds the node-specific context (ewidth of the go that made the potential, component names,
GAES history) and passes them on with the aiida-akaikkr palette. matplotlib is imported lazily.
"""
import os

# fixed categorical order (never cycled): blue, orange, aqua, yellow
SERIES = ["#2a78d6", "#eb6834", "#1baf7a", "#eda100"]
INK = "#0b0b0b"
INK2 = "#52514e"
GRID = "#d9d8d3"
L_NAMES = ["s", "p", "d", "f"]

# palette handed to pyakaikkr.plot (keys of pyakaikkr.plot.DEFAULT_STYLE)
STYLE = {"series": SERIES, "ink": INK, "ink2": INK2, "grid": GRID, "ewidth": SERIES[3], "final": SERIES[1],
         "coarse": SERIES[2], "fine": SERIES[0], "bounds": INK2, "level": INK2, "level_other": INK2,
         "linewidth": 1.2, "fill_alpha": 0.12, "coarse_alpha": 0.12, "fine_alpha": 0.18}


def _plt():
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    return plt


def _pk():
    from pyakaikkr import plot as pk_plot

    return pk_plot


def _style(ax, xlabel, ylabel, title, efermi=True):
    """axes decoration in the aiida-akaikkr look (pyakaikkr.plot.style_axes + E_F line)."""
    pk = _pk()
    pk.style_axes(ax, xlabel, ylabel, title, style=STYLE)
    if efermi:
        pk.mark_efermi(ax, STYLE)


def contour_bottom(node):
    """(E - E_F of the bottom of the SCF energy contour, label) for a dos node: -ewidth of the go CalcJob that
    made the potential (``inputs.potential.creator``), else -ewidth of the node itself.

    AkaiKKR integrates the charge over [E_F - ewidth, E_F] (cemesh.f), so states below this line were not
    included in the self-consistent charge; the DOS mesh of a dos run instead covers
    [E_F - ref*ewidth, E_F + (1 - ref)*ewidth] with ref = 0.75 by default (cemesr.f).
    """
    go = node.inputs.potential.creator if "potential" in node.inputs else None
    if go is not None and "results" in go.outputs and "ewidth" in go.outputs.results.keys():
        return -abs(float(go.outputs.results["ewidth"])), f"$-$ewidth of go (pk {go.pk})"
    return -abs(float(node.outputs.results["ewidth"])), "$-$ewidth"


def _mark_contour_bottom(ax, energy, node):
    """dash-dot vertical line at E - E_F = -|ewidth_go| (pyakaikkr.plot.mark_ewidth_go)."""
    ebtm, label = contour_bottom(node)
    _pk().mark_ewidth_go(ax, energy, -ebtm, STYLE, label=label)


def plot_dos(node, path, prefix):
    plt = _plt()
    energy = node.outputs.dos.get_array("energy")
    dos = node.outputs.dos.get_array("dos")  # (nspin, nenergy)
    ebtm, label = contour_bottom(node)
    fig, ax = plt.subplots(figsize=(7, 4))
    _pk().plot_dos(ax, energy, dos, ewidth_go=-ebtm, ewidth_label=label, style=STYLE, mirror_down=True,
                   fill=dos.shape[0] == 1, efermi=False)
    ax.legend(frameon=False)
    _style(ax, "$E - E_F$ (Ry)", "DOS (states/Ry)", f"{prefix}: total DOS")
    fig.tight_layout()
    fig.savefig(path, dpi=150)
    plt.close(fig)
    return path


def _component_names(node):
    return _pk().component_names(node.outputs.results["type_of_site"])


def plot_pdos(node, path, prefix):
    plt = _plt()
    energy = node.outputs.pdos.get_array("energy")
    pdos = node.outputs.pdos.get_array("pdos")  # (nspin, ncomponent, nenergy, nl)
    nspin, ncomp, _, _nl = pdos.shape
    names = _component_names(node)
    if len(names) != ncomp:
        names = [f"component {i}" for i in range(ncomp)]
    ebtm, label = contour_bottom(node)
    fig, axes = plt.subplots(ncomp, 1, figsize=(7, 3.2 * ncomp), squeeze=False)
    for ic, ax in enumerate(axes[:, 0]):
        _pk().plot_pdos(ax, energy, pdos[:, ic, :, :], ewidth_go=-ebtm, ewidth_label=label, style=STYLE,
                        l_names=L_NAMES, mirror_down=True, efermi=False)
        ax.legend(frameon=False)
        _style(ax, "$E - E_F$ (Ry)", "PDOS (states/Ry)" + (" (up +, down -)" if nspin > 1 else ""),
               f"{prefix}: PDOS {names[ic]}")
        ax.title.set_fontsize(9)
    fig.tight_layout()
    fig.savefig(path, dpi=150)
    plt.close(fig)
    return path


def plot_awk(node, outdir, prefix):
    from pyakaikkr import AwkReader

    plt = _plt()
    klabel = ["$" + str(list(v.keys())[0]) + "$" for path in node.outputs.klabel.get_dict()["kpath"] for v in path]
    written = []
    for port in ("Awk_up", "Awk_dn"):
        if port not in node.outputs:
            continue
        spin = port.split("_")[1]
        awkfile = node.outputs[port]
        with awkfile.open(awkfile.filename) as handle:
            awk = AwkReader(handle)
        fig, ax = plt.subplots(figsize=(8, 5))
        mesh = _pk().plot_awk(ax, awk.kdist, awk.energy, awk.Awk, awk.kcrt, klabel, style=STYLE, cmap="Blues",
                              vmax_percentile=99.5)
        ax.set_ylabel("$E - E_F$ (Ry)", color=INK)
        ax.set_title(f"{prefix}: A(w,k) spin {spin}", loc="left", color=INK, fontsize=11)
        ax.tick_params(colors=INK2)
        cb = fig.colorbar(mesh, ax=ax, pad=0.02)
        cb.set_label("A(w,k) (1/Ry)", color=INK)
        cb.outline.set_visible(False)
        fig.tight_layout()
        path = os.path.join(outdir, f"{prefix}_Awk_{spin}.png")
        fig.savefig(path, dpi=150)
        plt.close(fig)
        written.append(path)
    return written


def _component_shortnames(node):
    """{type name: [component short name, ...]} from results["type_of_site"] (e.g. Rh, Pt of a CPA type)."""
    names = {}
    for t in node.outputs.results["type_of_site"]:
        names[t["type"]] = _pk().component_names([t], long=False)
    return names


def jij_dataframe(node):
    """the `Jij` output Dict as a pandas DataFrame (columns as written by pyakaikkr get_jij_as_dataframe)."""
    import pandas as pd

    if "Jij" not in node.outputs:
        raise ValueError(f"Node<{node.pk}> has no `Jij` output (is it a jij CalcJob?)")
    df = pd.DataFrame(node.outputs.Jij.get_dict())
    for col in ("distance", "J_ij", "J_ij(meV)"):
        df[col] = df[col].astype(float)
    return df


def plot_jij(node, outdir, prefix, csv=True):
    """J_ij(R) figures: one PNG per type pair, one panel per (component1, component2) pair.

    Follows the AkaiKKRPythonUtil test script (JijPlotter.make_typepair / make_comppair): R is in units of the
    lattice constant `a` as printed by AkaiKKR, J_ij in meV, and all panels of a figure share the axes so the
    component pairs of a CPA type can be compared.  Optionally writes `<prefix>_jij.csv` (the same table).
    """
    import math

    plt = _plt()
    pk = _pk()
    df = jij_dataframe(node)
    short = _component_shortnames(node)
    tc = node.outputs.Tc.value if "Tc" in node.outputs else None
    written = []
    if csv:
        path = os.path.join(outdir, f"{prefix}_jij.csv")
        df.to_csv(path, index=False)
        written.append(path)
    xlim, ylim = pk.jij_limits(df["distance"], df["J_ij(meV)"])
    for (t1, t2), dft in df.groupby(["type1", "type2"], sort=False):
        pairs = list(dft.groupby(["comp1", "comp2"], sort=False))
        ncols = min(len(pairs), 3)
        nrows = math.ceil(len(pairs) / ncols)
        # sharey only: with sharex the x tick labels vanish on panels whose lower neighbour is hidden
        fig, axes = plt.subplots(nrows, ncols, figsize=(4.2 * ncols, 3.0 * nrows), squeeze=False, sharey=True)
        for ax in axes.flat[len(pairs):]:
            ax.set_visible(False)
        for ax, ((c1, c2), dfc) in zip(axes.flat, pairs):
            n1 = short.get(t1, [])
            n2 = short.get(t2, [])
            c1name = n1[int(c1) - 1] if 0 < int(c1) <= len(n1) else f"comp{c1}"
            c2name = n2[int(c2) - 1] if 0 < int(c2) <= len(n2) else f"comp{c2}"
            pk.plot_jij(ax, dfc["distance"], dfc["J_ij(meV)"], style=STYLE, xlim=xlim, ylim=ylim)
            _style(ax, "$R / a$", "$J_{ij}$ (meV)", f"{c1name}-{c2name}", efermi=False)
            ax.title.set_fontsize(10)
        title = f"{prefix}: $J_{{ij}}$ {t1} - {t2}"
        if tc is not None:
            title += f"   (Tc = {tc:.1f} K)"
        fig.suptitle(title, x=0.01, ha="left", color=INK, fontsize=11)
        fig.tight_layout(rect=(0, 0, 1, 0.95))
        path = os.path.join(outdir, f"{prefix}_Jij_{t1}-{t2}.png")
        fig.savefig(path, dpi=150)
        plt.close(fig)
        written.append(path)
    return written


def _gaes_bounds(parameters, entry=None):
    """[min_ewidth, max_ewidth] of a GAES judgement: the entry's own bounds (orbital rules re-derive them
    from the core levels) else the parameters (None = the defaults 1.0 / 2.0)."""
    if entry is not None and entry.get("orbital_bounds"):
        return tuple(entry["orbital_bounds"])
    lo, hi = parameters.get("min_ewidth"), parameters.get("max_ewidth")
    return (1.0 if lo is None else lo, 2.0 if hi is None else hi)


def plot_gaes(node, outdir, prefix):
    """DOS of every GAES iteration (one PNG each, plus an overview): coarse gap regions (green),
    fine sub-regions (blue), the [min_ewidth, max_ewidth] band (hatched), the core levels of the
    orbital rules, -ewidth of that go (red), the final ewidth (dashed) and the thresholds
    (per atom x natm, in the per-cell unit of the plot). Drawn by pyakaikkr.plot.plot_gaes_dos."""
    plt = _plt()
    pk = _pk()
    history = node.outputs.history.get_list()
    params = node.outputs.parameters.get_dict()
    final = node.outputs.ewidth.value if "ewidth" in node.outputs else None
    status = node.outputs.status.value if "status" in node.outputs else "running"
    written = []
    fig_all, ax_all = plt.subplots(figsize=(8, 4.2))
    for k, h in enumerate(history):
        dos_node = load_node(h["dos_pk"])
        energy = dos_node.outputs.dos.get_array("energy")
        dos = dos_node.outputs.dos.get_array("dos")
        fig, ax = plt.subplots(figsize=(8, 4.2))
        pk.plot_gaes_dos(ax, energy, dos, ewidth=h["ewidth"], ewidth_label="$-$ewidth of this go", final=final,
                         coarse=h.get("coarse_regions", []), fine=h.get("fine_regions", []),
                         bounds=_gaes_bounds(params, h), levels=h.get("orbital_levels", {}),
                         highlight=list(h.get("orbital_levels", {})), natm=h.get("natm"),
                         dosth=params.get("dosth", 2e-2), dosth2=params.get("dosth2", 1e-3), style=STYLE,
                         ylim=None, dos_label=f"total DOS, ewidth_go {h['ewidth']:.4f}", efermi=False)
        ax.legend(frameon=False, fontsize=8)
        _style(ax, "$E - E_F$ (Ry)", "DOS (states/Ry, spin sum)",
               f"{prefix}: GAES iew={h['iew']} ewidth={h['ewidth']:.4f} -> {h['flag']}"
               + (f" (next {h['candidates'][0]:.4f})" if h["flag"] == "new" and h["candidates"] else ""))
        fig.tight_layout()
        path = os.path.join(outdir, f"{prefix}_gaes{k:02d}_dos.png")
        fig.savefig(path, dpi=150)
        plt.close(fig)
        written.append(path)
        curve = dos.sum(axis=0) if dos.ndim == 2 else dos
        ax_all.plot(energy, curve, linewidth=1.2, color=SERIES[k % len(SERIES)],
                    label=f"iew={h['iew']} ewidth={h['ewidth']:.4f} ({h['flag']})")
    last = history[-1] if history else None
    pk.shade_ewidth_bounds(ax_all, _gaes_bounds(params, last), style=STYLE)
    if last is not None:
        pk.draw_levels(ax_all, last.get("orbital_levels", {}), highlight=list(last.get("orbital_levels", {})),
                       e_min=(last.get("window") or [None])[0], style=STYLE)
    if final is not None:
        ax_all.axvline(-final, color=INK, linewidth=1.0, linestyle="-.", label=f"$-$ewidth final ({final:.4f})")
    ax_all.set_yscale("log")
    ax_all.legend(frameon=False, fontsize=8)
    _style(ax_all, "$E - E_F$ (Ry)", "DOS (states/Ry, spin sum)", f"{prefix}: GAES {status}")
    fig_all.tight_layout()
    path = os.path.join(outdir, f"{prefix}_gaes_all.png")
    fig_all.savefig(path, dpi=150)
    plt.close(fig_all)
    written.append(path)
    return written


def load_node(pk):
    from aiida import orm

    return orm.load_node(int(pk))


def plot_cli(dos_pk=None, spc_pk=None, jij_pk=None, gaes_pk=None, outdir=None, prefix=None):
    from aiida import orm

    if not dos_pk and not spc_pk and not jij_pk and not gaes_pk:
        raise ValueError("give --dos-pk, --spc-pk, --jij-pk and/or --gaes-pk")
    written = []
    first = orm.load_node(int(dos_pk or spc_pk or jij_pk or gaes_pk))
    outdir = outdir or os.path.join(os.path.expanduser("~"), "aiida_work", "figures", str(first.pk))
    os.makedirs(outdir, exist_ok=True)
    prefix = prefix or (first.label.rsplit("_", 1)[0] if first.label else f"pk{first.pk}")
    if dos_pk:
        node = orm.load_node(int(dos_pk))
        if "dos" not in node.outputs:
            raise ValueError(f"Node<{node.pk}> has no `dos` output")
        written.append(plot_dos(node, os.path.join(outdir, f"{prefix}_dos.png"), prefix))
        if "pdos" in node.outputs:
            written.append(plot_pdos(node, os.path.join(outdir, f"{prefix}_pdos.png"), prefix))
    if spc_pk:
        node = orm.load_node(int(spc_pk))
        if "klabel" not in node.outputs:
            raise ValueError(f"Node<{node.pk}> has no `klabel` output (is it a spc CalcJob?)")
        written += plot_awk(node, outdir, prefix)
    if jij_pk:
        written += plot_jij(orm.load_node(int(jij_pk)), outdir, prefix)
    if gaes_pk:
        node = orm.load_node(int(gaes_pk))
        if "history" not in node.outputs:
            raise ValueError(f"Node<{node.pk}> has no `history` output (is it a finished GAES WorkChain?)")
        written += plot_gaes(node, outdir, prefix)
    return {"outdir": outdir, "prefix": prefix, "files": written}
