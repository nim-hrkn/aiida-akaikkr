"""
Parsers provided by aiida_diff.

Register parsers via the "aiida.parsers" entry point in setup.json.
"""
from aiida.common import exceptions
from aiida.engine import ExitCode
from aiida.parsers.parser import Parser
from aiida.plugins import CalculationFactory
from aiida.orm import Dict, Float
import aiida

from pyakaikkr import AkaikkrJob
from aiida.plugins import DataFactory

import numpy as np


aiida_major_version = int(aiida.__version__.split(".")[0])

SinglefileData = DataFactory('singlefile')
ArrayData = DataFactory('array')
FolderData = DataFactory('folder')
StructureData = DataFactory('structure')


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

        with open("/home/max/tmp/aiidalog", "a") as f:
            f.write('parse start\n')
        output_filename = self.node.get_option("output_filename")

        # Check that folder content is as expected
        if aiida_major_version == 2:
            output_folder = self.retrieved.base.repository
        elif aiida_major_version == 1:
            output_folder = self.retrieved
        else:
            raise ValueError("unknown aiida major verson. aiida version={aiida.__version__}")

        # TODO
        # Checking size must be done in case case.
        potential_filename = 'pot.dat'
        files_retrieved = output_folder.list_object_names()
        files_expected = [output_filename]
        if self.node.inputs.retrieve_potential.value:
            files_expected.append(potential_filename)
        # Note: set(A) <= set(B) checks whether A is a subset of B
        if not set(files_expected) <= set(files_retrieved):
            self.logger.error(
                f"Found files '{files_retrieved}', expected to find '{files_expected}'"
            )
            return self.exit_codes.ERROR_MISSING_OUTPUT_FILES

        if "spc" in self.node.inputs.go.value:
            awk_files_expected = 'pot.dat_*spc'
            from fnmatch import fnmatch
            have_spc = False
            for name in files_retrieved:
                if fnmatch(name, awk_files_expected):
                    have_spc = True
            if not have_spc:
                self.logger.error(f'failed to find spc files={awk_files_expected}')
                return self.exit_codes.ERROR_MISSING_OUTPUT_FILES

            klabel_files_expected = 'klabel.json'
            if klabel_files_expected not in files_retrieved:
                self.logger.error(f'failed to find file={klabel_files_expected}')
                return self.exit_codes.ERROR_MISSING_OUTPUT_FILES

        # add a potential file
        if self.node.inputs.retrieve_potential.value:
            with output_folder.open(potential_filename, "rb") as handle:
                potential = SinglefileData(file=handle)
                self.out('potential', potential)

        # add output file
        self.logger.info(f"Parsing '{output_filename}'")
        content = output_folder.get_object_content(output_filename).splitlines()
        output_node = get_basic_properties(content)
        self.out("results", Dict(dict=output_node))

        with output_folder.open(output_filename, "r") as handle:

            job = AkaikkrJob("dummy_directory")
            py_structure = job.make_pymatgenstructure(handle, change_atom_name=False)

            from pymatgen.io.ase import AseAtomsAdaptor
            aseadaptor = AseAtomsAdaptor()
            structure_output = True
            try:
                ase_structure = aseadaptor.get_atoms(py_structure)
            except ValueError:
                # ASE.Atoms don't accept lmd occupancy.
                structure_output = False
                self.logger.info('no structure output probably because magtyp=lmd.')
            if structure_output:
                structuredata = StructureData(ase=ase_structure)
                self.out('structure', structuredata)

        if self.node.inputs.go.value == 'dos':
            with output_folder.open(output_filename, "r") as handle:
                job = AkaikkrJob("dummy_directory")
                dosdata = job.get_dos(handle)
                dosarray = ArrayData()
                energy = np.array(dosdata[0])
                dos = np.array(dosdata[1])
                dosarray.set_array('energy', energy)
                dosarray.set_array('dos', dos)
                self.out('dos', dosarray)

            with output_folder.open(output_filename, "r") as handle:
                job = AkaikkrJob("dummy_directory")
                pdosdata = job.get_pdos(handle)
                pdosarray = ArrayData()
                energy = np.array(pdosdata[0])
                dos = np.array(pdosdata[1])
                pdosarray.set_array('energy', energy)
                pdosarray.set_array('pdos', dos)
                self.out('pdos', pdosarray)

        if self.node.inputs.go.value[:1] == 'j':
            with output_folder.open(output_filename, "r") as handle:
                job = AkaikkrJob("dummy_directory")
                df_jij = job.get_jij_as_dataframe(handle)
                jijarray = Dict()
                for name in df_jij.columns.tolist():
                    value = df_jij[name].values

                    jijarray[name] = value.tolist()
                self.out('Jij', jijarray)

        if self.node.inputs.go.value[:1] == 'j' or self.node.inputs.go.value == 'tc':
            with output_folder.open(output_filename, "r") as handle:
                tc = job.get_curie_temperature(handle)
                self.out('Tc', Float(tc))

        if self.node.inputs.go.value[:3] == 'spc':
            port_list = ["Awk_up", "Awk_dn"]
            postfix_list = ["up.spc", "dn.spc"]
            for portname, postfix in zip(port_list, postfix_list):
                name = "_".join(["pot.dat", postfix])
                if fnmatch(name, awk_files_expected):
                    with output_folder.open(name, "rb") as handle:
                        Awkfile = SinglefileData(handle)
                    self.out(portname, Awkfile)
            import json
            with output_folder.open("klabel.json", "r") as handle:
                klabel = json.load(handle)
                self.out('klabel', Dict(dict=klabel))

        if aiida_major_version == 2:
            return ExitCode(0)
