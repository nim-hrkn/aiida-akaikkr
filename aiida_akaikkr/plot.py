"""DOS / PDOS / A(w,k) / J_ij figures from finished aiida-akaikkr nodes (matplotlib imported lazily)."""
import os

# fixed categorical order (never cycled): blue, orange, aqua, yellow
SERIES = ["#2a78d6", "#eb6834", "#1baf7a", "#eda100"]
INK = "#0b0b0b"
INK2 = "#52514e"
GRID = "#d9d8d3"
L_NAMES = ["s", "p", "d", "f"]


def _plt():
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    return plt


def _style(ax, xlabel, ylabel, title, efermi=True):
    ax.set_xlabel(xlabel, color=INK)
    ax.set_ylabel(ylabel, color=INK)
    ax.set_title(title, loc="left", color=INK, fontsize=11)
    ax.grid(True, color=GRID, linewidth=0.6)
    for side in ("top", "right"):
        ax.spines[side].set_visible(False)
    for side in ("left", "bottom"):
        ax.spines[side].set_color(GRID)
    ax.tick_params(colors=INK2)
    if efermi:
        ax.axvline(0.0, color=INK2, linewidth=0.8, linestyle="--")


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
    """dashed vertical line at E - E_F = -|ewidth|; the x range is widened if the line is off the DOS mesh."""
    ebtm, label = contour_bottom(node)
    ax.axvline(ebtm, color=SERIES[3], linewidth=1.0, linestyle="-.", label=label)
    lo, hi = float(energy.min()), float(energy.max())
    if ebtm < lo:
        ax.set_xlim(ebtm - 0.02 * (hi - ebtm), hi + 0.02 * (hi - ebtm))


def plot_dos(node, path, prefix):
    plt = _plt()
    energy = node.outputs.dos.get_array("energy")
    dos = node.outputs.dos.get_array("dos")  # (nspin, nenergy)
    nspin = dos.shape[0]
    fig, ax = plt.subplots(figsize=(7, 4))
    if nspin == 1:
        ax.plot(energy, dos[0], color=SERIES[0], linewidth=1.6)
        ax.fill_between(energy, dos[0], color=SERIES[0], alpha=0.12, linewidth=0)
    else:
        ax.plot(energy, dos[0], color=SERIES[0], linewidth=1.6, label="up")
        ax.plot(energy, -dos[1], color=SERIES[1], linewidth=1.6, label="down")
        ax.axhline(0.0, color=INK2, linewidth=0.6)
    _mark_contour_bottom(ax, energy, node)
    ax.legend(frameon=False)
    _style(ax, "$E - E_F$ (Ry)", "DOS (states/Ry)", f"{prefix}: total DOS")
    fig.tight_layout()
    fig.savefig(path, dpi=150)
    plt.close(fig)
    return path


def _component_names(node):
    names = []
    for t in node.outputs.results["type_of_site"]:
        for c in t["comp_shortname"]:
            comp = c[len(t["type"]) + 1:] if c.startswith(t["type"] + "_") else c
            names.append(f'{comp.replace("_", " ")} in {t["type"]}' if comp != c else c)
    return names


def plot_pdos(node, path, prefix):
    import numpy as np

    plt = _plt()
    energy = node.outputs.pdos.get_array("energy")
    pdos = node.outputs.pdos.get_array("pdos")  # (nspin, ncomponent, nenergy, nl)
    nspin, ncomp, _, nl = pdos.shape
    names = _component_names(node)
    if len(names) != ncomp:
        names = [f"component {i}" for i in range(ncomp)]
    fig, axes = plt.subplots(ncomp, 1, figsize=(7, 3.2 * ncomp), squeeze=False)
    for ic, ax in enumerate(axes[:, 0]):
        for il in range(nl):
            if np.all(np.isnan(pdos[:, ic, :, il])):
                continue
            color = SERIES[il % len(SERIES)]
            label = L_NAMES[il] if il < len(L_NAMES) else f"l={il}"
            ax.plot(energy, pdos[0, ic, :, il], color=color, linewidth=1.6, label=label)
            if nspin > 1:
                ax.plot(energy, -pdos[1, ic, :, il], color=color, linewidth=1.6)
        if nspin > 1:
            ax.axhline(0.0, color=INK2, linewidth=0.6)
        _mark_contour_bottom(ax, energy, node)
        ax.legend(frameon=False)
        _style(ax, "$E - E_F$ (Ry)", "PDOS (states/Ry)" + (" (up +, down -)" if nspin > 1 else ""),
               f"{prefix}: PDOS {names[ic]}")
        ax.title.set_fontsize(9)
    fig.tight_layout()
    fig.savefig(path, dpi=150)
    plt.close(fig)
    return path


def plot_awk(node, outdir, prefix):
    import numpy as np
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
        mesh = ax.pcolormesh(awk.kdist, awk.energy, awk.Awk.T, cmap="Blues", shading="auto",
                             vmin=0.0, vmax=np.percentile(awk.Awk, 99.5))
        for x in awk.kdist[awk.kcrt]:
            ax.axvline(x, color=INK2, linewidth=0.6)
        ax.axhline(0.0, color=INK2, linewidth=0.8, linestyle="--")
        ax.set_xticks(awk.kdist[awk.kcrt])
        ax.set_xticklabels(klabel[:len(awk.kcrt)])
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
        short = []
        for c in t["comp_shortname"]:
            comp = c[len(t["type"]) + 1:] if c.startswith(t["type"] + "_") else c
            short.append(comp.split("_")[0] if comp != c else c)  # "Rh_50.0%" -> "Rh"
        names[t["type"]] = short
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
    df = jij_dataframe(node)
    short = _component_shortnames(node)
    tc = node.outputs.Tc.value if "Tc" in node.outputs else None
    written = []
    if csv:
        path = os.path.join(outdir, f"{prefix}_jij.csv")
        df.to_csv(path, index=False)
        written.append(path)

    xpad = 0.05 * (df["distance"].max() - df["distance"].min() or 1.0)
    xlim = (df["distance"].min() - xpad, df["distance"].max() + xpad)
    ypad = 0.05 * (df["J_ij(meV)"].max() - df["J_ij(meV)"].min() or 1.0)
    ylim = (df["J_ij(meV)"].min() - ypad, df["J_ij(meV)"].max() + ypad)

    for (t1, t2), dft in df.groupby(["type1", "type2"], sort=False):
        pairs = list(dft.groupby(["comp1", "comp2"], sort=False))
        ncols = min(len(pairs), 3)
        nrows = math.ceil(len(pairs) / ncols)
        # sharey only: with sharex the x tick labels vanish on panels whose lower neighbour is hidden
        fig, axes = plt.subplots(nrows, ncols, figsize=(4.2 * ncols, 3.0 * nrows), squeeze=False, sharey=True)
        for ax in axes.flat[len(pairs):]:
            ax.set_visible(False)
        for ax, ((c1, c2), dfc) in zip(axes.flat, pairs):
            dfc = dfc.sort_values("distance")
            ax.plot(dfc["distance"], dfc["J_ij(meV)"], color=SERIES[0], linewidth=1.4, marker="o", markersize=4)
            ax.axhline(0.0, color=INK2, linewidth=0.8, linestyle="--")
            n1 = short.get(t1, [])
            n2 = short.get(t2, [])
            c1name = n1[int(c1) - 1] if 0 < int(c1) <= len(n1) else f"comp{c1}"
            c2name = n2[int(c2) - 1] if 0 < int(c2) <= len(n2) else f"comp{c2}"
            _style(ax, "$R / a$", "$J_{ij}$ (meV)", f"{c1name}-{c2name}", efermi=False)
            ax.set_xlim(xlim)
            ax.set_ylim(ylim)
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


def plot_gaes(node, outdir, prefix):
    """DOS of every GAES iteration (one PNG each, plus an overview): coarse gap regions (green),
    fine sub-regions (blue), -ewidth of that go (red) and the final ewidth (dashed)."""
    import numpy as np

    plt = _plt()
    history = node.outputs.history.get_list()
    final = node.outputs.ewidth.value if "ewidth" in node.outputs else None
    status = node.outputs.status.value if "status" in node.outputs else "running"
    written = []
    fig_all, ax_all = plt.subplots(figsize=(8, 4.2))
    for k, h in enumerate(history):
        dos_node = load_node(h["dos_pk"])
        energy = dos_node.outputs.dos.get_array("energy")
        dos = np.asarray(dos_node.outputs.dos.get_array("dos"), dtype=float)
        curve = dos.sum(axis=0) if dos.ndim == 2 else dos
        fig, ax = plt.subplots(figsize=(8, 4.2))
        ax.plot(energy, curve, color=SERIES[0], linewidth=1.4, label=f"total DOS, ewidth_go {h['ewidth']:.4f}")
        for a, b in h.get("coarse_regions", []):
            ax.axvspan(a, b, color=SERIES[2], alpha=0.12)
        for a, b in h.get("fine_regions", []):
            ax.axvspan(a, b, color=SERIES[0], alpha=0.18)
        ax.axvline(-h["ewidth"], color=SERIES[3], linewidth=1.0, linestyle="-.", label="$-$ewidth of this go")
        if final is not None and abs(final - h["ewidth"]) > 1e-6:
            ax.axvline(-final, color=SERIES[1], linewidth=1.0, linestyle="--", label=f"$-$ewidth final ({final:.4f})")
        for th, ls in ((node.outputs.parameters.get("dosth", 2e-2), "--"), (node.outputs.parameters.get("dosth2", 1e-3), ":")):
            ax.axhline(th, color=INK2, linewidth=0.6, linestyle=ls)
        ax.set_yscale("log")
        ax.legend(frameon=False, fontsize=8)
        _style(ax, "$E - E_F$ (Ry)", "DOS (states/Ry, spin sum)",
               f"{prefix}: GAES iew={h['iew']} ewidth={h['ewidth']:.4f} -> {h['flag']}"
               + (f" (next {h['candidates'][0]:.4f})" if h["flag"] == "new" and h["candidates"] else ""))
        fig.tight_layout()
        path = os.path.join(outdir, f"{prefix}_gaes{k:02d}_dos.png")
        fig.savefig(path, dpi=150)
        plt.close(fig)
        written.append(path)
        ax_all.plot(energy, curve, linewidth=1.2, color=SERIES[k % len(SERIES)],
                    label=f"iew={h['iew']} ewidth={h['ewidth']:.4f} ({h['flag']})")
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
