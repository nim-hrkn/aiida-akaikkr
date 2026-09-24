"""Plot DOS, PDOS and A(w,k) from finished aiida-akaikkr CalcJob nodes.

Usage (inside the `akaikkr` conda env):

    python example/plot_results.py --dos <pk of specx_dos> --spc <pk of specx_spc> [--outdir figures] [--prefix Cu]

Writes <prefix>_dos.png, <prefix>_pdos.png and <prefix>_Awk_<spin>.png.
Energies are in Ry relative to the Fermi level, as written by AkaiKKR.
"""
import argparse
import os

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np

from aiida import load_profile, orm
from pyakaikkr import AwkReader

# fixed categorical order (never cycled): blue, orange, aqua, yellow
SERIES = ["#2a78d6", "#eb6834", "#1baf7a", "#eda100"]
INK = "#0b0b0b"
INK2 = "#52514e"
GRID = "#d9d8d3"
L_NAMES = ["s", "p", "d", "f"]


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


def plot_pdos(node, path, prefix):
    energy = node.outputs.pdos.get_array("energy")
    pdos = node.outputs.pdos.get_array("pdos")  # (nspin, ntype, nenergy, nl)
    # the second axis of pdos runs over CPA components (all types flattened)
    types = []
    for t in node.outputs.results["type_of_site"]:
        for c in t["comp_shortname"]:
            comp = c[len(t["type"]) + 1:] if c.startswith(t["type"] + "_") else c
            types.append(f'{comp.replace("_", " ")} in {t["type"]}' if comp != c else c)
    nspin, ntype, _, nl = pdos.shape
    if len(types) != ntype:
        types = [f"component {i}" for i in range(ntype)]
    fig, axes = plt.subplots(ntype, 1, figsize=(7, 3.2 * ntype), squeeze=False)
    for it, ax in enumerate(axes[:, 0]):
        for il in range(nl):
            if np.all(np.isnan(pdos[:, it, :, il])):
                continue  # this type has fewer l channels (NaN padded)
            color = SERIES[il % len(SERIES)]
            label = L_NAMES[il] if il < len(L_NAMES) else f"l={il}"
            ax.plot(energy, pdos[0, it, :, il], color=color, linewidth=1.6, label=label)
            if nspin > 1:
                ax.plot(energy, -pdos[1, it, :, il], color=color, linewidth=1.6)
        if nspin > 1:
            ax.axhline(0.0, color=INK2, linewidth=0.6)
        ax.legend(frameon=False, title=None)
        _style(ax, "$E - E_F$ (Ry)", "PDOS (states/Ry)" + (" (up +, down -)" if nspin > 1 else ""),
               f"{prefix}: PDOS {types[it]}")
        ax.title.set_fontsize(9)
    fig.tight_layout()
    fig.savefig(path, dpi=150)
    plt.close(fig)


def plot_awk(node, outdir, prefix):
    klabel = []
    for vpath in node.outputs.klabel.get_dict()["kpath"]:
        for v in vpath:
            k = str(list(v.keys())[0])
            klabel.append("$" + k + "$")
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


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dos", type=int, required=True, help="pk of the specx_dos node")
    ap.add_argument("--spc", type=int, required=True, help="pk of the specx_spc node")
    ap.add_argument("--outdir", default="figures")
    ap.add_argument("--prefix", default="Cu")
    args = ap.parse_args()

    load_profile()
    os.makedirs(args.outdir, exist_ok=True)
    dos_node = orm.load_node(args.dos)
    spc_node = orm.load_node(args.spc)

    written = []
    p = os.path.join(args.outdir, f"{args.prefix}_dos.png")
    plot_dos(dos_node, p, args.prefix); written.append(p)
    p = os.path.join(args.outdir, f"{args.prefix}_pdos.png")
    plot_pdos(dos_node, p, args.prefix); written.append(p)
    written += plot_awk(spc_node, args.outdir, args.prefix)
    for w in written:
        print("wrote", w)


if __name__ == "__main__":
    main()
