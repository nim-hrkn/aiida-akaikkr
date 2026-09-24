"""DOS / PDOS / A(w,k) figures from finished aiida-akaikkr nodes (matplotlib imported lazily)."""
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


def _style(ax, xlabel, ylabel, title):
    ax.set_xlabel(xlabel, color=INK)
    ax.set_ylabel(ylabel, color=INK)
    ax.set_title(title, loc="left", color=INK, fontsize=11)
    ax.grid(True, color=GRID, linewidth=0.6)
    for side in ("top", "right"):
        ax.spines[side].set_visible(False)
    for side in ("left", "bottom"):
        ax.spines[side].set_color(GRID)
    ax.tick_params(colors=INK2)
    ax.axvline(0.0, color=INK2, linewidth=0.8, linestyle="--")


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


def plot_cli(dos_pk=None, spc_pk=None, outdir=None, prefix=None):
    from aiida import orm

    if not dos_pk and not spc_pk:
        raise ValueError("give --dos-pk and/or --spc-pk")
    written = []
    first = orm.load_node(int(dos_pk or spc_pk))
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
    return {"outdir": outdir, "prefix": prefix, "files": written}
