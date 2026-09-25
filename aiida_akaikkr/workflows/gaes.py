"""AkaikkrGaesWorkChain: Gap-Anchored Ewidth Search (GAES) with AiiDA.

STEP1 loop of pyakaikkr.gaes: go (fresh potential) -> dos (from the go potential, fixed small
edelt) -> judge the gap regions of the DOS with pyakaikkr.gaes.decide (Method 1 or 2) ->
"old": finished, "new": next ewidth, "fail": ewidth_fail. The DOS is judged whether or not
the SCF converged. The dos window is widened (up to ewidth_dos_max) when the only low-DOS
region touches the bottom of the window. No SCF tightening (STEP2) in this version.
See AkaiKKRPythonUtil/docs/ewidth_tuning_scheme.md.
"""
import numpy as np
from aiida import orm
from aiida.engine import ToContext, WorkChain, while_

from ..inputs import build_calcjob
from ..query.nodes import output_nodes

GAES_DEFAULTS = {
    "ewidth_init": 1.2,       # Ry, initial ewidth of go
    "method": 2,              # 1: single threshold (2019), 2: coarse + fine thresholds
    "dosth": 2e-2,            # coarse threshold (states/Ry/cell, spin sum)
    "dosth2": 1e-3,           # fine threshold of Method 2
    "dosth2_relax": 2.0,      # dosth2 is relaxed by this factor when no fine sub-region exists
    "eth": 0.30,              # Ry, minimum gap width
    "ediff": 0.20,            # Ry, margin below the upper edge of the gap
    "margin": 0.01,           # Ry, extra margin of a new ewidth
    "min_ewidth": None,       # Ry, ewidth is chosen inside [min, max] (moved to the bound); gap regions
                              # themselves are judged over the whole dos window, independent of the bounds.
    "max_ewidth": None,       # None = the defaults 1.0 / 2.0, which `orbitals` rules replace (section 15)
    "orbitals": [],           # per-orbital rules, e.g. ["Rb4p=valence", "Bi6s=core"] (occupied / unoccupied aliases)
    "ef_assumed": 0.6,        # Ry, E_F assumed with the atomic level table before the first go
    "dos_per_atom": True,     # judge the DOS per atom (total DOS / natm); False = raw DOS per cell (2019). Plots keep per cell
    "max_ew": 10,             # maximum number of ewidth values tried
    "ewidth_dos": 3.0,        # Ry, dos window parameter (auto-widened so that the window reaches -ewidth-eth-ediff)
    "ewidth_dos_auto": True,
    "ewidth_dos_max": 4.5,    # never wider (numerical garbage below ~ -3 Ry)
    "ref": 0.75,              # cemesr ref of the build: akaikkr 0.75, akaikkr_cnd / cpa2021v01 0.5
    "edelt_init": 1e-4,       # edelt of go
    "edelt_dos": 1e-4,        # edelt of every judgement dos
    "maxitr": 500,            # maxitr of go
    "pmix": 0.005,            # pmix of go
}


DEFAULT_MIN_EWIDTH, DEFAULT_MAX_EWIDTH = 1.0, 2.0


def _spin_sum(dos_node):
    energy = np.asarray(dos_node.get_array("energy"), dtype=float)
    dos = np.asarray(dos_node.get_array("dos"), dtype=float)
    return energy, dos.sum(axis=0) if dos.ndim == 2 else dos


class AkaikkrGaesWorkChain(WorkChain):
    """GAES: find the ewidth of go that puts E_F - ewidth in the band gap between valence and semicore states."""

    @classmethod
    def define(cls, spec):
        super().define(spec)
        spec.input("code", valid_type=orm.AbstractCode, help="specx code")
        spec.input("common", valid_type=orm.Dict, help="common parameters (structure + parameters), see inputs.make_common_param")
        spec.input("gaes", valid_type=orm.Dict, default=lambda: orm.Dict(dict={}),
                   help="GAES parameters overriding GAES_DEFAULTS (ewidth_init, method, dosth, dosth2, eth, ediff, "
                        "min_ewidth, max_ewidth, orbitals, max_ew, ewidth_dos, ref, edelt_init, edelt_dos, maxitr, pmix, ...)")
        spec.input("displc", valid_type=orm.Bool, default=lambda: orm.Bool(False))
        spec.input("ncores", valid_type=orm.Int, default=lambda: orm.Int(8), help="OpenMP threads per job")
        spec.input("wallclock", valid_type=orm.Int, default=lambda: orm.Int(7200))
        spec.input("overrides", valid_type=orm.Dict, default=lambda: orm.Dict(dict={}),
                   help="AkaiKKR parameters applied to every job (e.g. bzqlty, sdftyp, option)")
        spec.input("label", valid_type=orm.Str, default=lambda: orm.Str("gaes"), help="label prefix of the jobs")

        spec.outline(cls.setup, while_(cls.should_continue)(cls.run_go, cls.inspect_go, cls.run_dos, cls.inspect_dos,
                                                            cls.judge), cls.finalize)

        spec.output("ewidth", valid_type=orm.Float, help="final ewidth of go (the anchored one, or the last tried)")
        spec.output("status", valid_type=orm.Str, help="finished | ewidth_fail | ewidth_exhausted | not_converged")
        spec.output("history", valid_type=orm.List, help="one entry per judgement: ewidth, flag, regions, candidates, pks")
        spec.output("parameters", valid_type=orm.Dict, help="GAES parameters actually used")
        spec.output_namespace("go", dynamic=True, required=False, help="outputs of the final go")
        spec.output_namespace("dos", dynamic=True, required=False, help="outputs of the final dos")

        spec.exit_code(400, "ERROR_GO_FAILED", message="a go calculation did not finish successfully")
        spec.exit_code(401, "ERROR_DOS_FAILED", message="a dos calculation did not finish successfully")
        spec.exit_code(420, "ERROR_EWIDTH_FAIL", message="no gap region wide enough was found (status ewidth_fail)")
        spec.exit_code(421, "ERROR_EWIDTH_EXHAUSTED", message="max_ew ewidth values tried without anchoring")
        spec.exit_code(422, "ERROR_NO_CANDIDATE", message="the remaining candidates were all tried already")
        spec.exit_code(423, "ERROR_ORBITAL_RULE", message="an orbital rule cannot be parsed or satisfied (see the report)")

    # ---------------------------------------------------------------- helpers
    def _p(self, key):
        return self.ctx.params[key]

    def _label(self, mode, iew):
        return f"{self.inputs.label.value}_gaes{iew:02d}_{mode}"

    def _ewidth_dos_for(self, ewidth):
        p = self.ctx.params
        need = (ewidth + p["eth"] + p["ediff"]) / p["ref"]
        ew = p["ewidth_dos"]
        if p["ewidth_dos_auto"] and need > ew:
            ew = round(need + 1e-9, 4)
        return min(ew, p["ewidth_dos_max"])

    # ---------------------------------------------------------------- outline
    def setup(self):
        from pyakaikkr.Error import GaesError
        from pyakaikkr.gaes import parse_orbital_rules, levels_for_step0, initial_ewidth

        params = dict(GAES_DEFAULTS)
        params.update(self.inputs.gaes.get_dict())
        self.ctx.params = params
        self.ctx.ewidth = float(params["ewidth_init"])
        self.ctx.levels = {}         # levels of the last go (mismatch check)
        self.ctx.levels_seen = {}    # every level seen in this run (bounds: shallowest for core, deepest for valence)
        bounds0 = None
        try:
            self.ctx.rules = parse_orbital_rules(params.get("orbitals") or [])
            if self.ctx.rules:
                # step 0: a first range from the level tables; every go output replaces it
                levels0 = levels_for_step0({r.element for r in self.ctx.rules}, params["ef_assumed"])
                bounds0 = self._bounds(levels0, strict=False)
                self.ctx.ewidth = float(initial_ewidth(self.ctx.ewidth, bounds0, params["ediff"]))
                params["orbital_bounds_step0"] = bounds0.as_list()
                params["orbital_levels_step0"] = {k: [round(v.e, 4), v.source] for k, v in levels0.items()}
                self.report(f"orbital rules {[str(r) for r in self.ctx.rules]}: step-0 bounds {bounds0.as_list()} "
                            f"from {params['orbital_levels_step0']}; first ewidth {self.ctx.ewidth:.4f}")
        except GaesError as e:
            self.report(f"orbital rules: {e}")
            return self.exit_codes.ERROR_ORBITAL_RULE
        self.ctx.iew = 0
        self.ctx.tried = []
        self.ctx.history = []
        self.ctx.status = None
        self.ctx.redo_dos = False
        self.ctx.ewidth_dos_used = None
        self.ctx.widened = 0
        self.report(f"GAES start: ewidth_init={self.ctx.ewidth}, method={params['method']}, "
                    f"dosth={params['dosth']}, dosth2={params['dosth2']}, "
                    f"bounds {(bounds0 or self._bounds({}, strict=False)).as_list()}")

    def _bounds(self, levels, strict=True):
        """[min_ewidth, max_ewidth] of the decision: the orbital rules applied to `levels`, else the
        user / default bounds (pyakaikkr.gaes.bounds_from_rules). strict=False at step 0."""
        from pyakaikkr.gaes import bounds_from_rules

        p = self.ctx.params
        return bounds_from_rules(getattr(self.ctx, "rules", []), levels, p["ediff"], p["min_ewidth"], p["max_ewidth"],
                                 DEFAULT_MIN_EWIDTH, DEFAULT_MAX_EWIDTH, strict=strict)

    def should_continue(self):
        return self.ctx.status is None and self.ctx.iew < self._p("max_ew")

    def run_go(self):
        if self.ctx.redo_dos:
            return
        p = self.ctx.params
        overrides = dict(self.inputs.overrides.get_dict())
        overrides.update({"ewidth": self.ctx.ewidth, "edelt": p["edelt_init"], "maxitr": p["maxitr"], "pmix": p["pmix"],
                          "record": "init"})   # a new ewidth always starts from a fresh potential
        builder = build_calcjob(self.inputs.code, "go", self.inputs.common, overrides=overrides,
                                displc=self.inputs.displc.value, ncores=self.inputs.ncores.value,
                                wallclock=self.inputs.wallclock.value, label=self._label("go", self.ctx.iew))
        node = self.submit(builder)
        self.report(f"iew={self.ctx.iew} ewidth={self.ctx.ewidth:.4f}: submitted go <{node.pk}>")
        return ToContext(go=node)

    def inspect_go(self):
        if self.ctx.redo_dos:
            return
        go = self.ctx.go
        if not go.is_finished_ok or "potential" not in go.outputs:
            self.report(f"go <{go.pk}> failed with exit status {go.exit_status}")
            return self.exit_codes.ERROR_GO_FAILED
        conv = bool(go.outputs.results.get("convervence")) if "results" in go.outputs else None
        self.ctx.go_converged = conv
        self.ctx.levels = {}
        if self.ctx.rules:
            from pyakaikkr.gaes import levels_from_records

            records = go.outputs.results.get("core_levels") if "results" in go.outputs else None
            if not records:
                self.report(f"go <{go.pk}>: no core_levels in the parsed results (update the parser); orbital rules cannot be checked")
                return self.exit_codes.ERROR_ORBITAL_RULE
            self.ctx.levels = levels_from_records(records)
            self.ctx.levels_seen = levels_from_records(records, self.ctx.levels_seen)
        self.report(f"go <{go.pk}> done, converged={conv}")

    def run_dos(self):
        p = self.ctx.params
        if self.ctx.redo_dos:
            ewidth_dos = self.ctx.ewidth_dos_used
        else:
            reach = self.ctx.ewidth
            if self.ctx.rules:
                # a valence rule may put min_ewidth below this go's contour: the window must reach it
                lo = self._bounds(self.ctx.levels_seen, strict=False).min_ewidth
                reach = max(reach, lo or 0.0)
            ewidth_dos = self._ewidth_dos_for(reach)
        self.ctx.ewidth_dos_used = ewidth_dos
        overrides = dict(self.inputs.overrides.get_dict())
        overrides.update({"ewidth": ewidth_dos, "edelt": p["edelt_dos"], "record": "2nd"})
        builder = build_calcjob(self.inputs.code, "dos", self.inputs.common, overrides=overrides,
                                potential=self.ctx.go.outputs.potential, displc=self.inputs.displc.value,
                                ncores=self.inputs.ncores.value, wallclock=self.inputs.wallclock.value,
                                label=self._label("dos", self.ctx.iew))
        node = self.submit(builder)
        self.report(f"iew={self.ctx.iew}: submitted dos <{node.pk}> (ewidth_dos={ewidth_dos}, edelt={p['edelt_dos']})")
        return ToContext(dos=node)

    def inspect_dos(self):
        dos = self.ctx.dos
        if not dos.is_finished_ok or "dos" not in dos.outputs:
            self.report(f"dos <{dos.pk}> failed with exit status {dos.exit_status}")
            return self.exit_codes.ERROR_DOS_FAILED

    def judge(self):
        from pyakaikkr.gaes import decide

        from pyakaikkr.Error import GaesError
        from pyakaikkr.gaes import check_rules, levels_as_dict

        p = self.ctx.params
        energy, curve = _spin_sum(self.ctx.dos.outputs.dos)
        natm = None
        if p.get("dos_per_atom", True):
            res = self.ctx.go.outputs.results.get_dict() if "results" in self.ctx.go.outputs else {}
            natm = len(res.get("atom_names") or []) or int(self.inputs.common.get_dict().get("natm") or 1)
            curve = curve / float(natm)
        try:
            bounds = self._bounds(self.ctx.levels_seen)
        except GaesError as e:
            self.report(f"orbital rules at ewidth {self.ctx.ewidth:.4f}: {e}")
            return self.exit_codes.ERROR_ORBITAL_RULE
        mismatch = check_rules(self.ctx.rules, self.ctx.levels) if self.ctx.rules else []
        dec = decide(p["method"], energy, [curve], self.ctx.ewidth, dosth=p["dosth"], dosth2=p["dosth2"], eth=p["eth"],
                     ediff=p["ediff"], margin=p["margin"], dosth2_relax=p["dosth2_relax"],
                     min_ewidth=bounds.min_ewidth, max_ewidth=bounds.max_ewidth)
        if mismatch and dec.flag == "old":
            # the go does not treat the orbitals as asked (a level moved across E_F - ewidth): the bounds
            # above were re-derived from the new levels, so move to the next candidate
            self.report(f"orbital rules not satisfied at ewidth {self.ctx.ewidth:.4f}: {mismatch}; bounds now {bounds.as_list()}")
            dec.flag = "new" if dec.candidates else "fail"
            dec.gap_used = None
        entry = {"iew": self.ctx.iew, "ewidth": self.ctx.ewidth, "converged": self.ctx.go_converged,
                 "orbital_bounds": bounds.as_list(), "orbital_levels": levels_as_dict(self.ctx.levels),
                 "orbital_mismatch": mismatch, "reasons": list(getattr(dec, "reasons", [])),
                 "dos_unit": "per_atom" if natm else "per_cell", "natm": natm,
                 "flag": dec.flag, "ewidth_dos": self.ctx.ewidth_dos_used,
                 "window": [float(energy.min()), float(energy.max())],
                 "coarse_regions": [list(g.as_tuple()) for g in dec.coarse],
                 "fine_regions": [list(f.as_tuple()) for f in dec.fine],
                 "candidates": [float(c) for c in dec.candidates],
                 "gap_used": list(dec.gap_used.as_tuple()) if dec.gap_used else None,
                 "relaxed": dec.relaxed, "dosth2_used": dec.dosth2_used, "window_limited": dec.window_limited,
                 "go_pk": self.ctx.go.pk, "dos_pk": self.ctx.dos.pk}
        self.ctx.redo_dos = False
        # the only low-DOS region touches the window bottom: widen the dos window and judge again
        if dec.flag == "fail" and dec.window_limited and p["method"] == 2:
            current = self.ctx.ewidth_dos_used
            wider = round(min(current * 1.5, p["ewidth_dos_max"]), 4)
            if wider > current + 1e-6 and self.ctx.widened < 3:
                self.ctx.widened += 1
                self.ctx.ewidth_dos_used = wider
                self.ctx.redo_dos = True
                entry["flag"] = "window_limited"
                self.ctx.history.append(entry)
                self.report(f"iew={self.ctx.iew}: low-DOS region touches the window bottom; "
                            f"widening ewidth_dos {current} -> {wider}")
                return
        self.ctx.history.append(entry)
        self.ctx.tried.append(round(self.ctx.ewidth, 6))
        self.report(f"iew={self.ctx.iew} ewidth={self.ctx.ewidth:.4f} converged={self.ctx.go_converged}: {dec.flag} "
                    f"bounds={entry['orbital_bounds']} coarse={entry['coarse_regions']} fine={entry['fine_regions']} "
                    f"cands={entry['candidates']}")
        if dec.flag == "old":
            self.ctx.status = "finished"
            return
        if dec.flag == "fail":
            self.ctx.status = "ewidth_fail"
            self.report("ewidth_fail: " + ("; ".join(entry["reasons"]) or "no reason recorded"))
            return
        nxt = None
        for c in dec.candidates:
            if round(c, 6) not in self.ctx.tried:
                nxt = c
                break
        if nxt is None:
            self.ctx.status = "not_converged"
            return
        self.ctx.ewidth = float(nxt)
        self.ctx.iew += 1
        self.ctx.widened = 0
        if self.ctx.iew >= p["max_ew"]:
            self.ctx.status = "ewidth_exhausted"

    def finalize(self):
        status = self.ctx.status or "ewidth_exhausted"
        self.out("ewidth", orm.Float(self.ctx.ewidth).store())
        self.out("status", orm.Str(status).store())
        self.out("history", orm.List(list=self.ctx.history).store())
        self.out("parameters", orm.Dict(dict=self.ctx.params).store())
        for port, node in output_nodes(self.ctx.go).items():
            self.out(f"go.{port}", node)
        for port, node in output_nodes(self.ctx.dos).items():
            self.out(f"dos.{port}", node)
        self.report(f"GAES {status}: ewidth={self.ctx.ewidth:.4f} after {len(self.ctx.tried)} ewidth value(s)")
        if status == "ewidth_fail":
            return self.exit_codes.ERROR_EWIDTH_FAIL
        if status == "ewidth_exhausted":
            return self.exit_codes.ERROR_EWIDTH_EXHAUSTED
        if status == "not_converged":
            return self.exit_codes.ERROR_NO_CANDIDATE
