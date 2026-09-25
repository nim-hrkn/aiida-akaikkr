# Copyright (c) 2022-2026 Hiori Kino.
# Distributed under the terms of the Apache License, Version 2.0.
"""Compare results with an AkaiKKRPythonUtil reference JSON (tests/<set>/reference/ifort.json)."""
import json

LABEL_MAP = {"_spc": "_spc31", "_jij": "_j3.0"}
PROCESS_MODE = {"specx_go": "go", "specx_dos": "dos", "specx_spc": "spc31", "specx_jij": "j3.0",
                "specx_tc": "tc", "specx_fsm": "fsm", "specx_cnd": "cnd"}


def _ref_label(label):
    for old, new in LABEL_MAP.items():
        if label.endswith(old):
            return label[: -len(old)] + new
    return label


def _rdiff(a, b):
    if a is None or b is None:
        return None
    return abs(a - b) / max(abs(b), 1e-12)


def _label_of(node, material):
    if material:
        return f"{material}_{PROCESS_MODE.get(node.process_label, node.process_label)}"
    return node.label or f"pk{node.pk}"


def _summaries_from_pks(pks, material=None):
    from .nodes import _is_chain, _node, _results_summary

    out = {}
    for pk in pks:
        node = _node(pk)
        if _is_chain(node):
            for child in node.called:
                if (child.process_label or "").startswith("specx_"):
                    out[_label_of(child, material)] = _results_summary(child)
        else:
            out[_label_of(node, material)] = _results_summary(node)
    return out


def _summaries_from_json(path):
    res = json.load(open(path))
    out = {}
    for label, d in res.items():
        out[label] = {"pk": d.get("pk"), "exit_status": d.get("exit"), "total_energy": d.get("te"),
                      "total_moment": d.get("moment"), "Tc": d.get("Tc"), "resistivity": d.get("resis"),
                      "conductivity": d.get("cnd")}
    return out


def compare(summaries: dict, reference_json: str):
    ref = json.load(open(reference_json))["result"]
    rows, worst = [], 0.0
    for label, d in summaries.items():
        rl = _ref_label(label)
        r = ref.get(rl)
        if r is None:
            rows.append({"label": label, "reference_label": rl, "found": False})
            continue
        te = _rdiff(d.get("total_energy"), r.get("te"))
        worst = max(worst, te or 0.0)
        row = {"label": label, "reference_label": rl, "found": True, "exit_status": d.get("exit_status"),
               "te": d.get("total_energy"), "te_ref": r.get("te"), "te_rdiff": te,
               "moment": d.get("total_moment"), "moment_ref": r.get("tm")}
        if "Tc" in r:
            row.update({"Tc": d.get("Tc"), "Tc_ref": r.get("Tc")})
        if "resis" in r:
            row.update({"resistivity": d.get("resistivity"), "resistivity_ref": r.get("resis"),
                        "conductivity": d.get("conductivity"), "conductivity_ref": r.get("cnd")})
        rows.append(row)
    return {"reference_json": reference_json, "n": len(rows), "worst_te_rdiff": worst, "rows": rows}


def compare_cli(pks=None, result_json=None, reference_json=None, material=None):
    if not reference_json:
        raise ValueError("--reference-json is required")
    if pks:
        summaries = _summaries_from_pks([int(p) for p in str(pks).split(",") if p.strip()], material)
    elif result_json:
        summaries = _summaries_from_json(result_json)
    else:
        raise ValueError("give --pks or --result-json")
    return compare(summaries, reference_json)
