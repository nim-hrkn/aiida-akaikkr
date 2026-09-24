"""AkaikkrChainWorkChain: go, then the requested follow-up modes from its potential."""
from aiida import orm
from aiida.engine import ToContext, WorkChain

from ..inputs import build_calcjob
from ..presets import FOLLOWUP_MODES
from ..query.nodes import output_nodes


class AkaikkrChainWorkChain(WorkChain):
    """Run specx `go` and then dos / spc / tc / jij / fsm / cnd from the converged potential.

    Outputs are grouped by mode: `go.results`, `go.potential`, `dos.dos`, `spc.Awk_up`, ...
    """

    @classmethod
    def define(cls, spec):
        super().define(spec)
        spec.input("code", valid_type=orm.AbstractCode, help="specx code")
        spec.input("common", valid_type=orm.Dict, help="common parameters (structure + parameters), see inputs.make_common_param")
        spec.input("modes", valid_type=orm.List, help="follow-up modes after go: dos, spc, tc, jij, fsm, cnd")
        spec.input("fspin", valid_type=orm.Float, default=lambda: orm.Float(1.0), help="fixed spin moment for fsm")
        spec.input("fsm_from_potential", valid_type=orm.Bool, default=lambda: orm.Bool(False),
                   help="fsm restarts from the go potential (record=2nd) instead of record=init")
        spec.input("displc", valid_type=orm.Bool, default=lambda: orm.Bool(False))
        spec.input("ncores", valid_type=orm.Int, default=lambda: orm.Int(8), help="OpenMP threads per job")
        spec.input("wallclock", valid_type=orm.Int, default=lambda: orm.Int(7200))
        spec.input("overrides", valid_type=orm.Dict, default=lambda: orm.Dict(dict={}),
                   help="AkaiKKR parameters applied to every job after the mode defaults")
        spec.input("spc_structure", valid_type=orm.StructureData, required=False,
                   help="structure for the spc k-path (default: the structure output of go)")
        spec.input("label", valid_type=orm.Str, default=lambda: orm.Str("akaikkr"), help="label prefix of the jobs")

        spec.outline(cls.run_go, cls.inspect_go, cls.run_followups, cls.inspect_followups)

        spec.output_namespace("go", dynamic=True)
        for mode in FOLLOWUP_MODES:
            spec.output_namespace(mode, dynamic=True, required=False)

        spec.exit_code(400, "ERROR_GO_FAILED", message="the go calculation did not finish successfully")
        spec.exit_code(401, "ERROR_NO_STRUCTURE_FOR_SPC",
                       message="spc requested but neither spc_structure nor a structure output of go is available")
        spec.exit_code(402, "ERROR_FOLLOWUP_FAILED", message="at least one follow-up calculation failed")
        spec.exit_code(410, "ERROR_UNKNOWN_MODE", message="modes contains an unknown mode")

    def _label(self, mode):
        return f"{self.inputs.label.value}_{mode}"

    def run_go(self):
        modes = self.inputs.modes.get_list()
        bad = [m for m in modes if m not in FOLLOWUP_MODES]
        if bad:
            self.report(f"unknown modes {bad}; known {FOLLOWUP_MODES}")
            return self.exit_codes.ERROR_UNKNOWN_MODE
        builder = build_calcjob(self.inputs.code, "go", self.inputs.common,
                                overrides=self.inputs.overrides.get_dict(), displc=self.inputs.displc.value,
                                ncores=self.inputs.ncores.value, wallclock=self.inputs.wallclock.value,
                                label=self._label("go"))
        node = self.submit(builder)
        self.report(f"submitted go <{node.pk}>")
        return ToContext(go=node)

    def inspect_go(self):
        go = self.ctx.go
        for port, node in output_nodes(go).items():
            self.out(f"go.{port}", node)
        if not go.is_finished_ok:
            self.report(f"go <{go.pk}> failed with exit status {go.exit_status}")
            return self.exit_codes.ERROR_GO_FAILED

    def run_followups(self):
        go = self.ctx.go
        futures = {}
        for mode in self.inputs.modes.get_list():
            kwargs = dict(overrides=self.inputs.overrides.get_dict(), displc=self.inputs.displc.value,
                          ncores=self.inputs.ncores.value, wallclock=self.inputs.wallclock.value,
                          label=self._label(mode), potential=go.outputs.potential)
            if mode == "fsm":
                kwargs["fspin"] = self.inputs.fspin.value
                kwargs["from_potential"] = self.inputs.fsm_from_potential.value
                if not self.inputs.fsm_from_potential.value:
                    kwargs["potential"] = None
            if mode == "spc":
                structure = self.inputs.get("spc_structure")
                if structure is None and "structure" in go.outputs:
                    structure = go.outputs.structure
                if structure is None:
                    self.report("spc requested but no structure is available (lmd go gives none)")
                    return self.exit_codes.ERROR_NO_STRUCTURE_FOR_SPC
                kwargs["structure_data"] = structure
            node = self.submit(build_calcjob(self.inputs.code, mode, self.inputs.common, **kwargs))
            self.report(f"submitted {mode} <{node.pk}>")
            futures[mode] = node
        return ToContext(**futures)

    def inspect_followups(self):
        failed = []
        for mode in self.inputs.modes.get_list():
            node = self.ctx[mode]
            for port, out in output_nodes(node).items():
                self.out(f"{mode}.{port}", out)
            if not node.is_finished_ok:
                failed.append((mode, node.pk, node.exit_status))
        if failed:
            self.report(f"failed follow-ups: {failed}")
            return self.exit_codes.ERROR_FOLLOWUP_FAILED
