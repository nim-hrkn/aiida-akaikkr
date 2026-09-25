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
    "min_ewidth": 1.0,        # Ry, ewidth is chosen inside [min, max] (moved to the bound); gap regions
                              # themselves are judged over the whole dos window, independent of the bounds
    "max_ewidth": 2.0,
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
                        "min_ewidth, max_ewidth, max_ew, ewidth_dos, ref, edelt_init, edelt_dos, maxitr, pmix, ...)")
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
        params = dict(GAES_DEFAULTS)
        params.update(self.inputs.gaes.get_dict())
        self.ctx.params = params
        self.ctx.ewidth = float(params["ewidth_init"])
        self.ctx.iew = 0
        self.ctx.tried = []
        self.ctx.history = []
        self.ctx.status = None
        self.ctx.redo_dos = False
        self.ctx.ewidth_dos_used = None
        self.ctx.widened = 0
        self.report(f"GAES start: ewidth_init={self.ctx.ewidth}, method={params['method']}, "
                    f"dosth={params['dosth']}, dosth2={params['dosth2']}, [{params['min_ewidth']}, {params['max_ewidth']}]")

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
        self.report(f"go <{go.pk}> done, converged={conv}")

    def run_dos(self):
        p = self.ctx.params
        if self.ctx.redo_dos:
            ewidth_dos = self.ctx.ewidth_dos_used
        else:
            ewidth_dos = self._ewidth_dos_for(self.ctx.ewidth)
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

        p = self.ctx.params
        energy, curve = _spin_sum(self.ctx.dos.outputs.dos)
        dec = decide(p["method"], energy, [curve], self.ctx.ewidth, dosth=p["dosth"], dosth2=p["dosth2"], eth=p["eth"],
                     ediff=p["ediff"], margin=p["margin"], dosth2_relax=p["dosth2_relax"],
                     min_ewidth=p["min_ewidth"], max_ewidth=p["max_ewidth"])
        entry = {"iew": self.ctx.iew, "ewidth": self.ctx.ewidth, "converged": self.ctx.go_converged,
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
                    f"coarse={entry['coarse_regions']} fine={entry['fine_regions']} cands={entry['candidates']}")
        if dec.flag == "old":
            self.ctx.status = "finished"
            return
        if dec.flag == "fail":
            self.ctx.status = "ewidth_fail"
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
