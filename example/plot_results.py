"""Plot DOS, PDOS, A(w,k) and J_ij(R) from finished aiida-akaikkr CalcJob nodes.

Thin wrapper around `aiida_akaikkr.plot` (also available as `akaikkr-aiida plot`).

    python example/plot_results.py --dos <pk of specx_dos> --spc <pk of specx_spc> --jij <pk of specx_jij> [--outdir figures] [--prefix Fe]

`--jij` writes <prefix>_jij.csv (the J_ij table) and one <prefix>_Jij_<type1>-<type2>.png per type pair,
with one panel per component pair (CPA), like the Jij_*.png of the AkaiKKRPythonUtil test script.
"""
import argparse

from aiida import load_profile

from aiida_akaikkr.plot import plot_cli


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dos", type=int, help="pk of the specx_dos node")
    ap.add_argument("--spc", type=int, help="pk of the specx_spc node")
    ap.add_argument("--jij", type=int, help="pk of the specx_jij node")
    ap.add_argument("--outdir", default="figures")
    ap.add_argument("--prefix", default=None)
    args = ap.parse_args()
    load_profile()
    for path in plot_cli(dos_pk=args.dos, spc_pk=args.spc, jij_pk=args.jij, outdir=args.outdir, prefix=args.prefix)["files"]:
        print("wrote", path)


if __name__ == "__main__":
    main()
