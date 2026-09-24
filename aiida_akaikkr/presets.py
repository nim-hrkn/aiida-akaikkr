"""Material presets (the AkaiKKRPythonUtil test set) and the mode table.

No aiida import here: this module is read by the CLI spec and by the MCP server.
"""
import os

# keys of the AkaiKKR input that describe the structure; the rest go to `parameters`
STRUCTURE_KEYS = frozenset({"brvtyp", "a", "c/a", "b/a", "alpha", "beta", "gamma", "r1", "r2", "r3",
                            "ntyp", "type", "ncmp", "rmt", "field", "mxl", "anclr", "conc",
                            "natm", "atmicx", "displc"})

# mode -> (calculation entry point, value of the `go` input)
MODES = {
    "go": ("akaikkr.go", "go"),
    "dos": ("akaikkr.dos", "dos"),
    "spc": ("akaikkr.spc", "spc31"),
    "tc": ("akaikkr.tc", "tc"),
    "jij": ("akaikkr.jij", "j3.0"),
    "fsm": ("akaikkr.fsm", "fsm"),
    "cnd": ("akaikkr.cnd", " cnd"),   # the leading space is required by specx
}
FOLLOWUP_MODES = [m for m in MODES if m != "go"]

# name: common-param function of akaikkr_testscript.testrun_class, CIF, follow-up modes, fspin
MATERIALS = {
    "Cu":           dict(func="_Cu_common_param",           cif="Cu-Fm3m.cif",       modes=["dos", "spc"], fspin=None),
    "Fe":           dict(func="_Fe_common_param",           cif="Fe-Im3m.cif",       modes=["fsm", "tc", "jij", "dos", "spc"], fspin=1.0),
    "Co":           dict(func="_Co_common_param",           cif="Co_P63mmc.cif",     modes=["fsm", "tc", "jij", "dos", "spc"], fspin=1.0),
    "Ni":           dict(func="_Ni_common_param",           cif="Ni-Fm3m.cif",       modes=["fsm", "tc", "jij", "dos", "spc"], fspin=1.0),
    "NiFe":         dict(func="_NiFe_common_param",         cif="NiFe-Fm3m.cif",     modes=["fsm", "tc", "jij", "dos", "spc"], fspin=1.0),
    "FeRh05Pt05":   dict(func="_FeRh05Pt05_common_param",   cif="FeRh0.5Pt0.5.cif",  modes=["fsm", "tc", "jij", "dos", "spc"], fspin=3.0),
    "AlMnFeCo_bcc": dict(func="_AlMnFeCo_bcc_common_param", cif="AlMnFeCo-Im3m.cif", modes=["fsm", "tc", "jij", "dos", "spc"], fspin=1.0),
    "Fe_lmd":       dict(func="_Fe_lmd_common_param",       cif="Fe-Im3m.cif",       modes=["dos"], fspin=None),
    "FeB195":       dict(func="_FeB195_common_param",       cif="FeB1.95-P6mmm.cif", modes=["dos", "spc"], fspin=None),
    "GaAs":         dict(func="_GaAs_common_param",         cif="GaAsVc-F43m.cif",   modes=["dos", "spc"], fspin=None),
    "Co2MnSi":      dict(func="_Co2MnSi_common_param",      cif="Co2MnSi-Fm3m.cif",  modes=["fsm", "tc", "jij", "dos", "spc"], fspin=4.5),
    "SmCo5_oc":     dict(func="_SmCo5_oc_common_param",     cif="SmCo5_P6mmm.cif",   modes=["fsm", "tc", "jij", "dos", "spc"], fspin=6.5),
    "SmCo5_noc":    dict(func="_SmCo5_noc_common_param",    cif="SmCo5_P6mmm.cif",   modes=[], fspin=None),
}
# fsm of these materials restarts from the go potential (record=2nd), as in the test script
FSM_FROM_GO_POTENTIAL = frozenset({"FeRh05Pt05", "Co2MnSi", "SmCo5_oc"})
# materials with a cnd step in the akaikkr_cnd test set
CND_MATERIALS = frozenset({"NiFe", "FeRh05Pt05", "AlMnFeCo_bcc"})
# lmd runs give no structure output; spc takes the k-path structure from another material's go
SPC_STRUCTURE_FROM = {"Fe_lmd": "Fe"}


def structure_dir() -> str:
    """directory of the preset CIF files (example/structure of the repository)."""
    env = os.environ.get("AKAIKKR_STRUCTURE_DIR")
    if env:
        return env
    return os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "example", "structure")


def preset_cif_path(name: str) -> str:
    return os.path.join(structure_dir(), MATERIALS[name]["cif"])


def preset_modes(name: str, displc: bool) -> list:
    modes = list(MATERIALS[name]["modes"])
    if displc and name in CND_MATERIALS:
        modes.append("cnd")
    return modes


def mode_parameter_overrides(mode: str, fspin: float | None, from_potential: bool) -> dict:
    """parameters that each mode sets on top of the common parameters (same as pyakaikkr.GoGo)."""
    if mode == "go":
        return {"record": "init"}
    if mode == "dos":
        return {"record": "2nd", "ewidth": 2.0}
    if mode in ("tc", "jij", "spc"):
        return {"record": "2nd"}
    if mode == "fsm":
        return {"record": "2nd" if from_potential else "init", "fspin": fspin, "pmix": "0.02ch"}
    if mode == "cnd":
        return {"record": "2nd", "ewidth": 0.01, "bzqlty": 40}
    raise ValueError(f"unknown mode {mode!r}; known: {list(MODES)}")
