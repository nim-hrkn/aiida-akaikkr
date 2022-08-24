"""
Parsers provided by aiida_diff.

Register parsers via the "aiida.parsers" entry point in setup.json.
"""
from aiida.common import exceptions
from aiida.engine import ExitCode
from aiida.parsers.parser import Parser
from aiida.plugins import CalculationFactory
from aiida.orm import Dict
import aiida

from pyakaikkr import AkaikkrJob

aiida_major_version = int(aiida.__version__.split(".")[0])


def _get_basic_properties(output_card: (str, list), get_history: bool = True):
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
    results["atom_coord"] = job.get_atom_coord(output_card)
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


specxCalculation = CalculationFactory("akaikkr.calcjob")


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
        output_filename = self.node.get_option("output_filename")

        # Check that folder content is as expected
        if aiida_major_version == 2:
            output_folder = self.retrieved.base.repository
        else:
            output_folder = self.retrieved

        files_retrieved = output_folder.list_object_names()
        files_expected = [output_filename]
        # Note: set(A) <= set(B) checks whether A is a subset of B
        if not set(files_expected) <= set(files_retrieved):
            self.logger.error(
                f"Found files '{files_retrieved}', expected to find '{files_expected}'"
            )
            return self.exit_codes.ERROR_MISSING_OUTPUT_FILES

        # add output file
        self.logger.info(f"Parsing '{output_filename}'")
        content = output_folder.get_object_content(output_filename).splitlines()
        output_node = _get_basic_properties(content)
        self.out("results", Dict(dict=output_node))

        if aiida_major_version == 2:
            return ExitCode(0)
