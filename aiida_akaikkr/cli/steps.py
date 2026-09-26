# Copyright (c) 2022-2026 Hiori Kino.
# Distributed under the terms of the Apache License, Version 2.0.
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


def _common_from(code_node, structure_pk=None, comp=None, polytyp=None, lattice=None, magtype=None, preset=None,
                 cif_path=None, displc=False, what="--structure-pk, --comp, --preset or --cif-path"):
    """the common-parameter Dict of a submission: an existing pk, a single-site CPA composition, a preset or a CIF."""
    from aiida import orm

    from ..inputs import generic_common_param, preset_common_param, single_site_common_param

    if structure_pk:
        return _node(structure_pk)
    if comp:
        return single_site_common_param(orm.Str(comp), orm.Str(polytyp or "fcc"), orm.Str(lattice or "expr"),
                                        orm.Str(magtype or "mag"))
    if preset:
        return preset_common_param(preset, code_node, displc, cif_path=cif_path)
    if cif_path:
        return generic_common_param(cif_path, code_node, displc)
    raise ValueError(f"give {what}")


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


def submit_go(structure_pk=None, comp=None, polytyp=None, lattice=None, magtype=None, code=None, displc=False,
              ncores=None, wallclock=None, label=None, parameters=None, caller="cli"):
    from aiida.engine import submit

    from ..inputs import build_calcjob

    code_node = _load_code(code)
    common = _common_from(code_node, structure_pk=structure_pk, comp=comp, polytyp=polytyp, lattice=lattice,
                          magtype=magtype, what="--structure-pk or --comp")
    builder = build_calcjob(code_node, "go", common, overrides=_overrides(parameters), displc=displc,
                            ncores=ncores or 8, wallclock=wallclock or 7200, label=label or (comp and comp + "_go") or "go")
    node = submit(builder)
    info = _job_info(node, code_node)
    info["common_pk"] = common.pk
    logdir.append_jsonl("action", {"action": "submit", "mode": "go", "pk": node.pk, "comp": comp, "caller": caller})
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


def submit_chain(structure_pk=None, preset=None, cif_path=None, comp=None, polytyp=None, lattice=None, magtype=None,
                 modes=None, fspin=None, spc_structure_pk=None, parameters=None, code=None, displc=False,
                 ncores=None, wallclock=None, label=None, caller="cli"):
    from aiida import orm
    from aiida.engine import submit

    from ..workflows.chain import AkaikkrChainWorkChain

    code_node = _load_code(code)
    common = _common_from(code_node, structure_pk=structure_pk, comp=comp, polytyp=polytyp, lattice=lattice,
                          magtype=magtype, preset=preset, cif_path=cif_path, displc=displc)

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
                  overrides=orm.Dict(dict=_overrides(parameters)),
                  label=orm.Str(label or comp or preset or "akaikkr"))
    if spc_structure_pk:
        inputs["spc_structure"] = _node(spc_structure_pk)
    if preset in SPC_STRUCTURE_FROM and "spc" in mode_list and not spc_structure_pk:
        raise ValueError(f"preset {preset} has no structure output for spc; submit spc separately with "
                         f"--spc-structure-pk from a {SPC_STRUCTURE_FROM[preset]} go")
    node = submit(AkaikkrChainWorkChain, **inputs)
    info = {"pk": node.pk, "process_label": node.process_label, "label": inputs["label"].value,
            "code": code_node.full_label, "common_pk": common.pk, "modes": mode_list,
            "fspin": inputs["fspin"].value, "fsm_from_potential": inputs["fsm_from_potential"].value,
            "spc_structure_pk": spc_structure_pk, "overrides": _overrides(parameters)}
    logdir.append_jsonl("action", {"action": "submit", "mode": "chain", "pk": node.pk, "modes": mode_list,
                                   "preset": preset, "comp": comp, "caller": caller})
    return info


def submit_gaes(structure_pk=None, preset=None, cif_path=None, comp=None, polytyp=None, lattice=None, magtype=None,
                ewidth_init=None, method=None, dosth=None, dosth2=None, min_ewidth=None, max_ewidth=None, orbital=None,
                max_ew=None, ewidth_dos=None, ref=None, parameters=None, code=None, displc=False, ncores=None, wallclock=None,
                label=None, caller="cli"):
    from aiida import orm
    from aiida.engine import submit

    from ..workflows.gaes import AkaikkrGaesWorkChain

    code_node = _load_code(code)
    common = _common_from(code_node, structure_pk=structure_pk, comp=comp, polytyp=polytyp, lattice=lattice,
                          magtype=magtype, preset=preset, cif_path=cif_path, displc=displc)
    gaes = {k: v for k, v in dict(ewidth_init=ewidth_init, method=method, dosth=dosth, dosth2=dosth2,
                                  min_ewidth=min_ewidth, max_ewidth=max_ewidth, max_ew=max_ew, ewidth_dos=ewidth_dos,
                                  ref=ref).items() if v is not None}
    if ref is None and "cnd" in code_node.label or ref is None and "cpa2021" in code_node.label:
        gaes["ref"] = 0.5
    if orbital:
        # "Rb4p=valence,Bi6s=core" (or a list): per-orbital rules deriving the ewidth range (section 15)
        items = orbital if isinstance(orbital, (list, tuple)) else str(orbital).split(",")
        gaes["orbitals"] = [it.strip() for it in items if it.strip()]
    inputs = dict(code=code_node, common=common, gaes=orm.Dict(dict=gaes), displc=orm.Bool(bool(displc)),
                  ncores=orm.Int(ncores or 8), wallclock=orm.Int(wallclock or 7200),
                  overrides=orm.Dict(dict=_overrides(parameters)),
                  label=orm.Str(label or comp or preset or "gaes"))
    node = submit(AkaikkrGaesWorkChain, **inputs)
    info = {"pk": node.pk, "process_label": node.process_label, "label": inputs["label"].value,
            "code": code_node.full_label, "common_pk": common.pk, "gaes": gaes}
    logdir.append_jsonl("action", {"action": "submit", "mode": "gaes", "pk": node.pk, "comp": comp, "preset": preset,
                                   "caller": caller})
    return info
