"""Plot DOS, PDOS and A(w,k) from finished aiida-akaikkr CalcJob nodes.

Thin wrapper around `aiida_akaikkr.plot` (also available as `akaikkr-aiida plot`).

    python example/plot_results.py --dos <pk of specx_dos> --spc <pk of specx_spc> [--outdir figures] [--prefix Cu]
"""
import argparse

from aiida import load_profile

from aiida_akaikkr.plot import plot_cli


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dos", type=int, help="pk of the specx_dos node")
    ap.add_argument("--spc", type=int, help="pk of the specx_spc node")
    ap.add_argument("--outdir", default="figures")
    ap.add_argument("--prefix", default=None)
    args = ap.parse_args()
    load_profile()
    for path in plot_cli(dos_pk=args.dos, spc_pk=args.spc, outdir=args.outdir, prefix=args.prefix)["files"]:
        print("wrote", path)


if __name__ == "__main__":
    main()
