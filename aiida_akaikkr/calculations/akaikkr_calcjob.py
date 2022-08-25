"""
Calculations provided by aiida_diff.

Register calculations via the "aiida.calculations" entry point in setup.json.
"""
from aiida.common import datastructures
from aiida.engine import CalcJob
from aiida.orm import Str, Dict, Bool, Int, Float
from aiida.plugins import DataFactory
from aiida.common.exceptions import InputValidationError

from pyakaikkr import AkaikkrJob
from pyakaikkr.HighsymmetryKpath import HighSymKPath

# from aiida.plugins import DataFactory
import os

SinglefileData = DataFactory('singlefile')
ArrayData = DataFactory('array')
FolderData = DataFactory('folder')
StructureData = DataFactory('structure')
RemoteData = DataFactory('remote')


def _copy_potential(calcinfo, potential, potential_filename):
    local_potential = None
    remote_potential_folder = None
    if isinstance(potential, Str):
        if len(potential.value) > 0:
            local_potential = SinglefileData(potential.value)
    elif isinstance(potential, SinglefileData):
        local_potential = potential
    elif isinstance(potential, RemoteData):
        remote_potential_folder = potential
    else:
        raise InputValidationError('unknown potential type. type={type(potential)}')

    if local_potential is not None:
        calcinfo.local_copy_list = [(local_potential.uuid, local_potential.filename, potential_filename)]
    if remote_potential_folder is not None:
        remote_potential_path = os.path.join(remote_potential_folder.get_remote_path(), potential_filename)
        calcinfo.remote_copy_list = [(remote_potential_folder.computer.uuid, remote_potential_path, ".")]


class specx_basic(CalcJob):
    """
    akaikkr Go.

    SCF class.
    """

    _RETRIEVE_POTENTIAL = True
    _POTENTIAL_FILE = "pot.dat"
    _GO = 'go'

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

        spec.input("go", valid_type=Str, help="kkr go parameter")
        spec.input("magtype", valid_type=Str, help="magtype")
        spec.input("structure", valid_type=Dict, help="kkr structure parameters in the akaikkr format",)
        spec.input("parameters", valid_type=Dict, help="kkr parameters",)
        spec.input("displc", valid_type=Bool, help="add displc or not.")
        spec.input("retrieve_potential", valid_type=Bool, default=lambda: Bool(cls._RETRIEVE_POTENTIAL),
                   help="retrieve potential file or not.")
        spec.input("potential", valid_type=(Str, SinglefileData, RemoteData), default=lambda: Str(""),
                   help="potential file.",)

        spec.output("results", valid_type=Dict, help="output properties.")
        spec.output("structure", valid_type=StructureData, help="structure really calculated.",)

        spec.exit_code(200, "ERROR_NO_RETRIEVED_FOLDER",
                       message="The retrieved folder data node could not be accessed.")

        # stdout file, or some file
        # versus
        # missing, read, parse, imcomplete
        spec.exit_code(302, 'ERROR_OUTPUT_STDOUT_MISSING',
                       message='The retrieved folder did not contain the required stdout output file.')
        spec.exit_code(310, 'ERROR_OUTPUT_STDOUT_READ',
                       message='The stdout output file could not be read.')
        spec.exit_code(311, 'ERROR_OUTPUT_STDOUT_PARSE',
                       message='The stdout output file could not be parsed.')
        spec.exit_code(312, 'ERROR_OUTPUT_STDOUT_INCOMPLETE',
                       message='The stdout output file was incomplete probably because the calculation got interrupted.')

        spec.exit_code(320, 'ERROR_OUTPUT_STDIN_MISSING',
                       message='The retrieved folder did not contain the required stdin output file.')

        spec.exit_code(321, 'ERROR_OUTPUT_POTENTIAL_MISSING',
                       message='The retrieved folder did not contain the required potential output file.')

        spec.exit_code(322, 'ERROR_OUTPUT_SPC_MISSING',
                       message='The retrieved folder did not contain the required spc output file.')

        spec.exit_code(323, 'ERROR_OUTPUT_KLABEL_MISSING',
                       message='The retrieved folder did not contain the required klabel output file.')

        spec.exit_code(324, 'ERROR_OUTPUT_DOS_PARSE',
                       message='The stdout output file could not be parsed for DOS.')
        spec.exit_code(325, 'ERROR_OUTPUT_PDOS_PARSE',
                       message='The stdout output file could not be parsed for PDOS.')
        spec.exit_code(326, 'ERROR_OUTPUT_JIJ_PARSE',
                       message='The stdout output file could not be parsed for Jij.')
        spec.exit_code(327, 'ERROR_OUTPUT_CURIE_TEMPERATURE_PARSE',
                       message='The stdout output file could not be parsed for Curie temperature.')

        # general parse

        spec.exit_code(390, 'ERROR_UNEXPECTED_PARSER_EXCEPTION',
                       message='The parser raised an unexpected exception.')

    def prepare_for_submission(self, folder):
        """
        Create input files.

        :param folder: an `aiida.common.folders.Folder` where the plugin should temporarily place all files
            needed by the calculation.
        :return: `aiida.common.datastructures.CalcInfo` instance
        """
        codeinfo = datastructures.CodeInfo()
        kkr_param = self.inputs.parameters.get_dict()
        kkr_param.update(self.inputs.structure.get_dict())
        kkr_param["magtyp"] = self.inputs.magtype.value
        kkr_param["go"] = self.inputs.go.value
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

        _copy_potential(calcinfo, self.inputs.potential, self._POTENTIAL_FILE)

        calcinfo.retrieve_list = [self.metadata.options.output_filename,
                                  self.metadata.options.input_filename]
        if self.inputs.retrieve_potential.value:
            calcinfo.retrieve_list.append(self._POTENTIAL_FILE)

        return calcinfo


class specx_go(specx_basic):
    """
    akaikkr Go.

    SCF class.
    """

    _RETRIEVE_POTENTIAL = True
    _GO = 'go'

    @classmethod
    def define(cls, spec):
        """Define inputs and outputs of the calculation.

        The type of input.potential is Str or SinglefileData. If it is Str, it must be an absolute path.
        """
        super().define(spec)

        # set default values for AiiDA options
        spec.expose_inputs(specx_basic)
        spec.input("go", valid_type=Str, help="kkr go parameter", default=lambda: Str(cls._GO))

        spec.expose_outputs(specx_basic)
        spec.output("potential", valid_type=SinglefileData, help="output potential file.",)

    def prepare_for_submission(self, folder):
        """
        Create input files.

        :param folder: an `aiida.common.folders.Folder` where the plugin should temporarily place all files
            needed by the calculation.
        :return: `aiida.common.datastructures.CalcInfo` instance
        """
        codeinfo = datastructures.CodeInfo()
        kkr_param = self.inputs.parameters.get_dict()
        kkr_param.update(self.inputs.structure.get_dict())
        kkr_param["magtyp"] = self.inputs.magtype.value
        kkr_param["go"] = self.inputs.go.value
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

        _copy_potential(calcinfo, self.inputs.potential, self._POTENTIAL_FILE)

        calcinfo.retrieve_list = [self.metadata.options.output_filename,
                                  self.metadata.options.input_filename]
        if self.inputs.retrieve_potential.value:
            calcinfo.retrieve_list.append(self._POTENTIAL_FILE)

        return calcinfo


class specx_fsm(specx_basic):
    """
    akaikkr Go.

    SCF class.
    """

    _RETRIEVE_POTENTIAL = True
    _POTENTIAL_FILE = "pot.dat"
    _GO = 'fsm'

    @classmethod
    def define(cls, spec):
        """Define inputs and outputs of the calculation.

        The type of input.potential is Str or SinglefileData. If it is Str, it must be an absolute path.
        """
        super().define(spec)

        # set default values for AiiDA options
        spec.expose_inputs(specx_basic)

        spec.input("go", valid_type=Str, help="kkr go parameter", default=lambda: Str(cls._GO))
        spec.input("fspin", valid_type=Float, help="fixed spin moment")

        spec.expose_outputs(specx_basic)
        spec.output("potential", valid_type=SinglefileData, help="output potential file.",)
        spec.output("structure", valid_type=StructureData, help="structure really calculated.",)

    def prepare_for_submission(self, folder):
        """
        Create input files.

        :param folder: an `aiida.common.folders.Folder` where the plugin should temporarily place all files
            needed by the calculation.
        :return: `aiida.common.datastructures.CalcInfo` instance
        """
        codeinfo = datastructures.CodeInfo()
        kkr_param = self.inputs.parameters.get_dict()
        kkr_param.update(self.inputs.structure.get_dict())
        kkr_param["go"] = self.inputs.go.value
        kkr_param["fspin"] = self.inputs.fspin.value
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

        _copy_potential(calcinfo, self.inputs.potential, self._POTENTIAL_FILE)

        calcinfo.retrieve_list = [self.metadata.options.output_filename,
                                  self.metadata.options.input_filename]

        if self.inputs.retrieve_potential.value:
            calcinfo.retrieve_list.append(self._POTENTIAL_FILE)

        return calcinfo


class specx_dos(specx_basic):
    """
    akaikkr dos.

    """

    _RETRIEVE_POTENTIAL = False
    _GO = 'dos'

    @ classmethod
    def define(cls, spec):
        """Define inputs and outputs of the calculation.

        The type of input.potential is Str or SinglefileData. If it is Str, it must be an absolute path.
        """
        super().define(spec)

        # set default values for AiiDA options
        spec.expose_inputs(specx_basic)
        spec.input("go", valid_type=Str, help="kkr go parameter", default=lambda: cls._GO)
        spec.input("retrieve_potential", valid_type=Bool, default=lambda: Bool(cls._RETRIEVE_POTENTIAL),
                   help="retrieve potential file or not.")
        spec.expose_outputs(specx_basic)
        spec.output("dos", valid_type=ArrayData, help="density of states.")
        spec.output("pdos", valid_type=ArrayData, help="partial density of states.")


class specx_jij(specx_basic):
    """
    akaikkr Jij.

    """

    _RETRIEVE_POTENTIAL = False
    _GO = 'j3.0'

    @ classmethod
    def define(cls, spec):
        """Define inputs and outputs of the calculation.

        The type of input.potential is Str or SinglefileData. If it is Str, it must be an absolute path.
        """
        super().define(spec)

        # set default values for AiiDA options
        spec.expose_inputs(specx_basic)
        spec.input("go", valid_type=Str, help="kkr go parameter", default=lambda: Str(cls._GO))
        spec.input("retrieve_potential", valid_type=Bool, default=lambda: Bool(cls._RETRIEVE_POTENTIAL),
                   help="retrieve potential file or not.")
        spec.expose_outputs(specx_basic)
        spec.output("Jij", valid_type=Dict, help="Jij")
        spec.output("Tc", valid_type=Float, help="Tc from the real space J_ij model.")


class specx_tc(specx_basic):
    """
    akaikkr Tc.

    """

    _RETRIEVE_POTENTIAL = False
    _GO = 'tc'

    @ classmethod
    def define(cls, spec):
        """Define inputs and outputs of the calculation.

        The type of input.potential is Str or SinglefileData. If it is Str, it must be an absolute path.
        """
        super().define(spec)

        # set default values for AiiDA options
        spec.expose_inputs(specx_basic)
        spec.input("go", valid_type=Str, help="kkr go parameter", default=lambda: Str(cls._GO))
        spec.input("retrieve_potential", valid_type=Bool, default=lambda: Bool(cls._RETRIEVE_POTENTIAL),
                   help="retrieve potential file or not.")
        spec.expose_outputs(specx_basic)
        spec.output("Tc", valid_type=Float, help="Tc from the k space J_ij model.")


class specx_spc(specx_basic):
    """
    akaikkr spc.

    A(w,k) class.
    """

    _RETRIEVE_POTENTIAL = False
    _GO = 'spc31'
    _KLABEL_FILENAME = "klabel.json"
    _NK = 150

    @classmethod
    def define(cls, spec):
        """Define inputs and outputs of the calculation.

        The type of input.potential is Str or SinglefileData. If it is Str, it must be an absolute path.
        """
        super().define(spec)

        # set default values for AiiDA options
        spec.expose_inputs(specx_basic)
        spec.input("go", valid_type=Str, help="kkr go parameter", default=lambda: Str(cls._GO))
        spec.input("retrieve_potential", valid_type=Bool, default=lambda: Bool(cls._RETRIEVE_POTENTIAL),
                   help="retrieve potential file or not.")
        spec.input("structure_data", valid_type=StructureData, help="structure really calculated.",)
        spec.input("nk", valid_type=Int, default=lambda: Int(cls._NK), help="number of k-points in A(w,k).",)
        spec.expose_outputs(specx_basic)
        spec.output("Awk_up", valid_type=SinglefileData, help="A(w,k) for spin up.",)
        spec.output("Awk_dn", valid_type=SinglefileData, help="A(w,k) for spin down.",)
        spec.output("klabel", valid_type=Dict, help="k labels of high symmetry points.",)

    def prepare_for_submission(self, folder):
        """
        Create input files.

        :param folder: an `aiida.common.folders.Folder` where the plugin should temporarily place all files
            needed by the calculation.
        :return: `aiida.common.datastructures.CalcInfo` instance
        """
        codeinfo = datastructures.CodeInfo()

        kkr_param = self.inputs.parameters.get_dict()
        kkr_param.update(self.inputs.structure.get_dict())
        kkr_param["magtyp"] = self.inputs.magtype.value
        kkr_param["go"] = self.inputs.go.value

        # kpath file must be made.
        klabel_filename = self._KLABEL_FILENAME
        with folder.open(klabel_filename, "w", encoding='utf8') as handle:
            struc = self.node.inputs.structure_data.get_pymatgen()
            highsymkpath = HighSymKPath(structure=struc,
                                        klabel_filename=handle)
            fmt = 3
            nk = self.inputs.nk.value
            kpath = highsymkpath.make_akaikkr_lines(nk=nk, fmt=fmt,
                                                    first_connected_kpath=False)
            kkr_param.update({"kpath_raw": kpath})

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

        _copy_potential(calcinfo, self.inputs.potential, self._POTENTIAL_FILE)

        calcinfo.retrieve_list = [self.metadata.options.output_filename,
                                  self.metadata.options.input_filename,
                                  klabel_filename,
                                  self._POTENTIAL_FILE+"_*"  # _up.spc and _dn.spc, which are results of calculation.
                                  ]

        if self.inputs.retrieve_potential.value:
            calcinfo.retrieve_list.append(self._POTENTIAL_FILE)

        return calcinfo
