"""Submit-side implementations of the CLI (structure, submit-go, submit-followup, submit-chain)."""
import json
import os

from .. import logdir
from ..presets import FSM_FROM_GO_POTENTIAL, MATERIALS, MODES, SPC_STRUCTURE_FROM, preset_modes
from ..query.nodes import input_nodes


def _load_code(label):
    from aiida import orm

    label = label or os.environ.get("AKAIKKR_CODE")
    if not label:
        raise ValueError("no code given: pass --code <label> or set AKAIKKR_CODE (see `codes`)")
    return orm.load_code(label)


def _node(pk):
    from aiida import orm

    return orm.load_node(int(pk))


def _overrides(parameters):
    if not parameters:
        return {}
    if isinstance(parameters, dict):
        return parameters
    d = json.loads(parameters)
    if not isinstance(d, dict):
        raise ValueError("--parameters must be a JSON object")
    return d


def _job_info(node, code):
    return {"pk": node.pk, "label": node.label, "process_label": node.process_label, "code": code.full_label,
            "inputs": {k: v.pk for k, v in input_nodes(node).items() if k != "code"},
            "resources": dict(node.get_option("resources")),
            "max_wallclock_seconds": node.get_option("max_wallclock_seconds")}


def structure_from_cif(cif_path=None, preset=None, code=None, displc=False, magtype=None, caller="cli"):
    from ..inputs import generic_common_param, preset_common_param, summarize_common

    if not cif_path and not preset:
        raise ValueError("give --cif-path and/or --preset")
    code_node = _load_code(code)
    if preset:
        node = preset_common_param(preset, code_node, displc, cif_path=cif_path, magtype=magtype or "")
    else:
        node = generic_common_param(cif_path, code_node, displc, magtype=magtype or "nmag")
    info = summarize_common(node)
    info.update({"preset": preset, "cif": cif_path or (MATERIALS[preset]["cif"] if preset else None),
                 "cif_pk": node.creator.inputs.cif.pk})
    return info


def submit_go(structure_pk, code=None, displc=False, ncores=None, wallclock=None, label=None, parameters=None,
              caller="cli"):
    from aiida.engine import submit

    from ..inputs import build_calcjob

    common = _node(structure_pk)
    code_node = _load_code(code)
    builder = build_calcjob(code_node, "go", common, overrides=_overrides(parameters), displc=displc,
                            ncores=ncores or 8, wallclock=wallclock or 7200, label=label or "go")
    node = submit(builder)
    info = _job_info(node, code_node)
    logdir.append_jsonl("action", {"action": "submit", "mode": "go", "pk": node.pk, "caller": caller})
    return info


def submit_followup(go_pk, mode, fspin=None, from_potential=False, spc_structure_pk=None, ncores=None,
                    wallclock=None, parameters=None, caller="cli"):
    from aiida.engine import submit

    from ..inputs import build_calcjob

    if mode not in MODES or mode == "go":
        raise ValueError(f"mode must be one of {[m for m in MODES if m != 'go']}")
    go = _node(go_pk)
    if not (go.process_label or "").startswith("specx_"):
        raise ValueError(f"Node<{go.pk}> is not a specx CalcJob (process_label={go.process_label})")
    if "potential" not in go.outputs:
        raise ValueError(f"Node<{go.pk}> has no `potential` output; it must be a finished go or fsm")
    common = go.inputs.structure.creator.inputs.common
    code_node = go.inputs.code
    potential = go.outputs.potential
    if mode == "fsm" and not from_potential:
        potential = None
    structure_data = None
    if mode == "spc":
        structure_data = _node(spc_structure_pk) if spc_structure_pk else (
            go.outputs.structure if "structure" in go.outputs else None)
        if structure_data is None:
            raise ValueError("spc needs a structure: the go has none (lmd?), pass --spc-structure-pk")
    label = go.label.rsplit("_", 1)[0] + f"_{mode}" if go.label else mode
    builder = build_calcjob(code_node, mode, common, fspin=fspin, from_potential=from_potential,
                            overrides=_overrides(parameters), potential=potential, structure_data=structure_data,
                            displc=go.inputs.displc.value, ncores=ncores or 8, wallclock=wallclock or 7200,
                            label=label)
    node = submit(builder)
    info = _job_info(node, code_node)
    info["go_pk"] = go.pk
    logdir.append_jsonl("action", {"action": "submit", "mode": mode, "pk": node.pk, "go_pk": go.pk, "caller": caller})
    return info


def submit_chain(structure_pk=None, preset=None, cif_path=None, modes=None, fspin=None, code=None, displc=False,
                 ncores=None, wallclock=None, label=None, caller="cli"):
    from aiida import orm
    from aiida.engine import submit

    from ..inputs import generic_common_param, preset_common_param
    from ..workflows.chain import AkaikkrChainWorkChain

    code_node = _load_code(code)
    if structure_pk:
        common = _node(structure_pk)
    elif preset:
        common = preset_common_param(preset, code_node, displc, cif_path=cif_path)
    elif cif_path:
        common = generic_common_param(cif_path, code_node, displc)
    else:
        raise ValueError("give --structure-pk, --preset or --cif-path")

    if modes:
        mode_list = [m.strip() for m in modes.split(",") if m.strip()]
    elif preset:
        mode_list = preset_modes(preset, displc)
    else:
        mode_list = ["dos", "spc"]
    if fspin is None and preset:
        fspin = MATERIALS[preset]["fspin"]

    inputs = dict(code=code_node, common=common, modes=orm.List(list=mode_list),
                  fspin=orm.Float(fspin if fspin is not None else 1.0),
                  fsm_from_potential=orm.Bool(bool(preset) and preset in FSM_FROM_GO_POTENTIAL),
                  displc=orm.Bool(bool(displc)), ncores=orm.Int(ncores or 8), wallclock=orm.Int(wallclock or 7200),
                  label=orm.Str(label or preset or "akaikkr"))
    if preset in SPC_STRUCTURE_FROM and "spc" in mode_list:
        raise ValueError(f"preset {preset} has no structure output for spc; submit spc separately with "
                         f"--spc-structure-pk from a {SPC_STRUCTURE_FROM[preset]} go")
    node = submit(AkaikkrChainWorkChain, **inputs)
    info = {"pk": node.pk, "process_label": node.process_label, "label": inputs["label"].value,
            "code": code_node.full_label, "common_pk": common.pk, "modes": mode_list,
            "fspin": inputs["fspin"].value, "fsm_from_potential": inputs["fsm_from_potential"].value}
    logdir.append_jsonl("action", {"action": "submit", "mode": "chain", "pk": node.pk, "modes": mode_list,
                                   "preset": preset, "caller": caller})
    return info
