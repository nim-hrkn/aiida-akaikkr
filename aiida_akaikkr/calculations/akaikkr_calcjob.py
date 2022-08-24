"""
Calculations provided by aiida_diff.

Register calculations via the "aiida.calculations" entry point in setup.json.
"""
from aiida.common import datastructures
from aiida.engine import CalcJob
from aiida.orm import Str, Dict, Bool
from aiida.plugins import DataFactory

from pyakaikkr import AkaikkrJob
from ..common.aiida_support import folder_prepare_object

# from aiida.plugins import DataFactory


SinglefileData = DataFactory('singlefile')


class specx_go(CalcJob):
    """
    akaikkr Go.

    SCF class.
    """

    _RETRIEVE_POTENTIAL = True
    _POTENTIAL_FILE = "pot.dat"

    @classmethod
    def define(cls, spec):
        """Define inputs and outputs of the calculation.

        The type of input.potential is Str or SinglefileData. If it is Str, it must be an absolute path.
        """
        super().define(spec)

        # set default values for AiiDA options
        spec.inputs["metadata"]["options"]["resources"].default = {
            "num_machines": 1,
            "num_mpiprocs_per_machine": 1,
        }
        spec.inputs["metadata"]["options"]["parser_name"].default = "akaikkr.parser"

        # new ports
        spec.input("metadata.options.input_filename", valid_type=str, default="go.in")
        spec.input("metadata.options.output_filename", valid_type=str, default="go.out")

        spec.input("parameters", valid_type=Dict, help="kkr parameters",)
        spec.input("displc", valid_type=Bool, help="add displc or not.")
        spec.input("retrieve_potential", valid_type=Bool, default=lambda: Bool(cls._RETRIEVE_POTENTIAL),
                   help="retrieve potential file or not.")
        spec.input("potential", valid_type=(Str, SinglefileData), default=lambda: Str(""),
                   help="potential file.",)

        spec.output("results", valid_type=Dict, help="output properties.",)
        spec.output("potential", valid_type=SinglefileData, help="output potential file.",)

        spec.exit_code(
            300,
            "ERROR_MISSING_OUTPUT_FILES",
            message="Calculation did not produce all expected output files.",
        )

    def prepare_for_submission(self, folder):
        """
        Create input files.

        :param folder: an `aiida.common.folders.Folder` where the plugin should temporarily place all files
            needed by the calculation.
        :return: `aiida.common.datastructures.CalcInfo` instance
        """
        codeinfo = datastructures.CodeInfo()
        kkr_param = self.inputs.parameters.get_dict()
        # make stdin file
        directory = "dummy"
        job = AkaikkrJob(directory)
        with folder.open(self.metadata.options.input_filename, "w", encoding='utf8') as handle:
            job.make_inputcard(kkr_param, handle)

        codeinfo.code_uuid = self.inputs.code.uuid
        codeinfo.cmdline_params = []
        codeinfo.stdin_name = self.metadata.options.input_filename
        codeinfo.stdout_name = self.metadata.options.output_filename
        codeinfo.withmpi = self.inputs.metadata.options.withmpi

        # Prepare a `CalcInfo` to be returned to the engine
        calcinfo = datastructures.CalcInfo()
        calcinfo.codes_info = [codeinfo]

        potential = None
        if isinstance(self.inputs.potential, Str):
            if len(self.inputs.potential.value) > 0:
                print("potential", self.inputs.potential.value)
                potential = SinglefileData(self.inputs.potential.value)
        elif isinstance(self.inputs.potential, SinglefileData):
            potential = self.inputs.potential
        if potential is not None:
            calcinfo.local_copy_list = [(potential.uuid, potential.filename, 'pot.dat')]

        calcinfo.retrieve_list = [self.metadata.options.output_filename,
                                  self.metadata.options.input_filename,
                                  'pot.dat']

        if self.inputs.retrieve_potential.value:
            calcinfo.retrieve_list.append(self._POTENTIAL_FILE)

        return calcinfo
