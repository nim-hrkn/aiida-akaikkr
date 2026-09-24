"""Compare a run_examples.py result JSON with an AkaiKKRPythonUtil reference JSON.

    python example/compare_reference.py run_examples_result.json <path>/tests/akaikkr_cnd/reference/ifort.json
"""
import json
import sys


def rdiff(a, b):
    if a is None or b is None:
        return None
    return abs(a - b) / max(abs(b), 1e-12)


def main(result_path, ref_path):
    res = json.load(open(result_path))
    ref = json.load(open(ref_path))["result"]
    print(f"{'label':22s} {'exit':>4s} {'te rdiff':>9s} {'moment(aiida/ref)':>22s} {'Tc(aiida/ref)':>20s} {'resis(aiida/ref)':>18s}")
    worst = 0.0
    for label, d in res.items():
        rl = label.replace("_spc", "_spc31")
        r = ref.get(rl)
        if r is None:
            print(f"{label:22s} {str(d['exit']):>4s} (no reference '{rl}')")
            continue
        te = rdiff(d.get("te"), r.get("te"))
        worst = max(worst, te or 0.0)
        mom = f"{d.get('moment')}/{r.get('tm')}"
        tc = f"{d.get('Tc', '')}/{r.get('Tc', '')}" if "Tc" in r else ""
        resis = f"{d.get('resis', '')}/{r.get('resis', '')}" if "resis" in r else ""
        print(f"{label:22s} {str(d['exit']):>4s} {te if te is None else f'{te:9.1e}':>9} {mom:>22s} {tc:>20s} {resis:>18s}")
        if "cnd" in r and d.get("cnd"):
            print(f"{'':22s}      cnd aiida={d['cnd']} ref={r['cnd']}")
    print(f"worst te rdiff = {worst:.1e}")


if __name__ == "__main__":
    main(sys.argv[1], sys.argv[2])
