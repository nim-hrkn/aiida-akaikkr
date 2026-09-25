"""
Parsers provided by aiida_diff.

Register parsers via the "aiida.parsers" entry point in setup.json.
"""
from aiida.common import exceptions
from aiida.engine import ExitCode
from aiida.parsers.parser import Parser
from aiida.plugins import CalculationFactory
from aiida.orm import Dict, Float, List
import aiida

from pyakaikkr import AkaikkrJob
from pyakaikkr.Error import KKRValueAquisitionError
from aiida.plugins import DataFactory

import numpy as np


aiida_major_version = int(aiida.__version__.split(".")[0])

SinglefileData = DataFactory('core.singlefile')
ArrayData = DataFactory('core.array')
FolderData = DataFactory('core.folder')
StructureData = DataFactory('core.structure')


def get_basic_properties(output_card: (str, list), get_history: bool = True):
    """get basic properties of akaikkr from output card

        output_card can be a filename or a list of string.

    Args:
        output_card (str,list): output card of akaikkr
        get_history (bool, optional): get history properties of not.

    Returns:
        dict: properties as dict.
    """
    directory = "dummy"
    job = AkaikkrJob(directory)
    results = {}
    results["convervence"] = job.get_convergence(output_card)
    if get_history:
        results["rms_error"] = job.get_rms_error(output_card)
        results["err_history"] = job.get_err_history(output_card)
        results["te_history"] = job.get_te_history(output_card)
        results["moment_history"] = job.get_moment_history(output_card)
    results["lattice_constant"] = job.get_lattice_constant(output_card)
    results["struc_param"] = job.get_struc_param(output_card)
    results["ntype"] = job.get_ntype(output_card)
    results["type_of_site"] = job.get_type_of_site(output_card)
    results["magtyp"] = job.get_magtyp(output_card)
    results["unitcell_volume"] = job.get_unitcell_volume(output_card)
    results["ewidth"] = job.get_ewidth(output_card)
    results["go"] = job.get_go(output_card)
    results["potentialfile"] = job.get_potentialfile(output_card)
    results["edelt"] = job.get_edelt(output_card)
    results["fermi_level"] = job.get_fermi_level(output_card)
    results["total_energy"] = job.get_total_energy(output_card)
    results["total_moment"] = job.get_total_moment(output_card)
    results["local_moment"] = job.get_local_moment(output_card)
    results["type_charge"] = job.get_type_charge(output_card)
    results["prim_vec"] = job.get_prim_vec(output_card)
    atom_coords, atom_names = job.get_atom_coord(output_card)
    results['atom_coords'] = atom_coords
    results['atom_names'] = atom_names
    core_names = ["1s", "2s", "2p", "3s", "3d", "4s", "4p", "4d", "4f"]
    core_exist, _core_level = job.get_core_level(output_card,  core_state=core_names)
    core_level = {}
    for s, e, l in zip(core_names, core_exist, _core_level):
        if e:
            core_level[s] = l
        else:
            core_level[s] = None
    results["core_level"] = core_level
    # per-component core levels with the '*' (valence) mark, E - E_F: used by the GAES orbital rules
    if hasattr(job, "get_core_levels_by_component"):
        results["core_levels"] = job.get_core_levels_by_component(output_card)
    return results


specxCalculation = CalculationFactory("akaikkr.basic")


class specx_parser(Parser):
    """
    specx parser class/
    """

    def __init__(self, node):
        """
        Initialize Parser instance

        Checks that the ProcessNode being passed was produced by a DiffCalculation.

        :param node: ProcessNode of calculation
        :param type node: :class:`aiida.orm.nodes.process.process.ProcessNode`
        """
        super().__init__(node)
        if not issubclass(node.process_class, specxCalculation):
            raise exceptions.ParsingError("Can only parse specxCalculation")

    def parse(self, **kwargs):
        """
        Parse outputs, store results in database.

        :returns: an exit code, if parsing fails (or nothing if parsing succeeds)
        """

        # Check that folder content is as expected
        if aiida_major_version == 2:
            output_folder = self.retrieved.base.repository
        elif aiida_major_version == 1:
            output_folder = self.retrieved
        else:
            raise ValueError(f"unknown aiida major version. aiida version={aiida.__version__}")

        files_retrieved = output_folder.list_object_names()

        output_filename = self.node.get_option("output_filename")
        if output_filename not in files_retrieved:
            return self.exit_codes.ERROR_OUTPUT_STDOUT_MISSING

        input_filename = self.node.get_option("input_filename")
        if input_filename not in files_retrieved:
            return self.exit_codes.ERROR_OUTPUT_STDIN_MISSING

        potential_filename = 'pot.dat'
        if self.node.inputs.retrieve_potential.value:
            if potential_filename not in files_retrieved:
                return self.exit_codes.ERROR_OUTPUT_POTENTIAL_MISSING

        magtype = self.node.inputs.magtype.value
        go = self.node.inputs.go.value.strip()  # " cnd" carries a leading space
        if "spc" in go:
            spc_postfixes = ["up"] if magtype == "nmag" else ["up", "dn"]
            for postfix in spc_postfixes:
                _potential_file = f'{potential_filename}_{postfix}.spc'
                if _potential_file not in files_retrieved:
                    return self.exit_codes.ERROR_OUTPUT_SPC_MISSING

            klabel_files_expected = 'klabel.json'
            if klabel_files_expected not in files_retrieved:
                return self.exit_codes.ERROR_OUTPUT_KLABEL_MISSING

        # add a potential file
        if self.node.inputs.retrieve_potential.value:
            with output_folder.open(potential_filename, "rb") as handle:
                potential = SinglefileData(file=handle)
                self.out('potential', potential)

        # parse output file
        self.logger.info(f"Parsing '{output_filename}'")
        content = output_folder.get_object_content(output_filename).splitlines()
        try:
            output_node = get_basic_properties(content)
        except KKRValueAquisitionError:
            return self.exit_codes.ERROR_OUTPUT_STDOUT_PARSE
        self.out("results", Dict(dict=output_node))

        if magtype != "lmd":
            with output_folder.open(output_filename, "r") as handle:
                job = AkaikkrJob("dummy_directory")
                py_structure = job.make_pymatgenstructure(handle, change_atom_name=False)
            # StructureData(pymatgen=...) keeps partial occupancies (CPA) as kinds with weights,
            # which the ASE route cannot represent.  Empty spheres (Z=0, written as Og by
            # pyakaikkr) are not an element AiiDA knows, so they are dropped here.
            empty = [i for i, site in enumerate(py_structure)
                     if len(site.species) == 0 or "Og" in site.species_string]
            if empty:
                self.logger.warning(f'{len(empty)} empty-sphere (Og) sites removed from the structure output')
                py_structure = py_structure.copy()
                py_structure.remove_sites(empty)
            try:
                structuredata = StructureData(pymatgen=py_structure)
            except Exception as exc:  # pylint: disable=broad-except
                self.logger.warning(f'structure output skipped: {exc}')
            else:
                self.out('structure', structuredata)

        if go == 'dos':
            with output_folder.open(output_filename, "r") as handle:
                job = AkaikkrJob("dummy_directory")
                try:
                    energy, dos = job.get_dos(handle)
                except KKRValueAquisitionError:
                    return self.exit_codes.ERROR_OUTPUT_DOS_PARSE
                dosarray = ArrayData()
                energy = np.array(energy)
                dos = np.array(dos)
                dosarray.set_array('energy', energy)
                dosarray.set_array('dos', dos)
                self.out('dos', dosarray)

            with output_folder.open(output_filename, "r") as handle:
                job = AkaikkrJob("dummy_directory")
                try:
                    energy,  dos = job.get_pdos(handle)
                except KKRValueAquisitionError:
                    return self.exit_codes.ERROR_OUTPUT_PDOS_PARSE
                pdosarray = ArrayData()
                energy = np.array(energy)
                # dos is [spin][type][energy][l]; the number of l depends on the type (mxl),
                # so pad with NaN to the largest l and record the number of l per type.
                nl_per_type = [len(dos[0][it][0]) for it in range(len(dos[0]))]
                nl_max = max(nl_per_type)
                padded = np.full((len(dos), len(dos[0]), len(energy), nl_max), np.nan)
                for ispin, per_type in enumerate(dos):
                    for it, per_energy in enumerate(per_type):
                        padded[ispin, it, :, :nl_per_type[it]] = np.array(per_energy)
                pdosarray.set_array('energy', energy)
                pdosarray.set_array('pdos', padded)
                pdosarray.set_array('nl_per_type', np.array(nl_per_type))
                self.out('pdos', pdosarray)

        if go[:1] == 'j':
            with output_folder.open(output_filename, "r") as handle:
                job = AkaikkrJob("dummy_directory")
                try:
                    df_jij = job.get_jij_as_dataframe(handle)
                except KKRValueAquisitionError:
                    return self.exit_codes.ERROR_OUTPUT_JIJ_PARSE
                jijarray = Dict()
                for name in df_jij.columns.tolist():
                    value = df_jij[name].values
                    jijarray[name] = value.tolist()
                self.out('Jij', jijarray)

        if go[:1] == 'j' or go == 'tc':
            with output_folder.open(output_filename, "r") as handle:
                job = AkaikkrJob("dummy_directory")
                try:
                    tc = job.get_curie_temperature(handle)
                except KKRValueAquisitionError:
                    return self.exit_codes.ERROR_OUTPUT_CURIE_TEMPERATURE_PARSE
                self.out('Tc', Float(tc))

        if go == 'cnd':
            with output_folder.open(output_filename, "r") as handle:
                job = AkaikkrJob("dummy_directory")
                try:
                    resistivity = job.get_resistivity(handle)
                    conductivity = job.get_conductivity(handle)
                except KKRValueAquisitionError:
                    return self.exit_codes.ERROR_OUTPUT_CND_PARSE
                self.out('resistivity', Float(resistivity))
                self.out('conductivity', List(list=conductivity))

        if go[:3] == 'spc':
            if magtype == "nmag":
                port_list = ["Awk_up"]
                postfix_list = ["up.spc"]
            else:
                port_list = ["Awk_up", "Awk_dn"]
                postfix_list = ["up.spc", "dn.spc"]
            for portname, postfix in zip(port_list, postfix_list):
                name = "_".join([potential_filename, postfix])
                with output_folder.open(name, "rb") as handle:
                    Awkfile = SinglefileData(handle)
                    # possibly the content is null.
                self.out(portname, Awkfile)
            import json
            with output_folder.open("klabel.json", "r") as handle:
                klabel = json.load(handle)
                # possibly the content is null.
                self.out('klabel', Dict(dict=klabel))

        if aiida_major_version == 2:
            return ExitCode(0)
