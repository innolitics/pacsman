"""
The Dcmtk client uses dcmtk binaries (pre-requisite) along with files written and read by pydicom.
It also has a storage SCP that runs at all times, as opposed to the transient listeners
spawned by PynetDicomClient.

DCMDICTPATH and (depending on the installation) SCPCFGPATH envrionment variables are
required.
"""
import logging
import os
import re
import subprocess
from subprocess import PIPE
import shutil
import tempfile
import threading
import glob
from collections import defaultdict

from typing import List, Optional, Iterable, Tuple

import pydicom
from pydicom import dcmread
from pydicom.dataset import Dataset

from .base_client import BaseDicomClient
from .utils import process_and_write_png_from_file, copy_dicom_attributes, \
    set_undefined_tags_to_blank, dicom_filename

logger = logging.getLogger(__name__)

# http://dicom.nema.org/medical/dicom/current/output/html/part07.html#chapter_C
status_success_or_pending = [0x0000, 0xFF00, 0xFF01]

dcmtk_error_codes = {
    # No data (timeout in non-blocking mode)
    'dcmnet-DIMSEC_NODATAAVAILABLE': (6, 0x207)
}
"""
This is a partial list of DCMTK error code constants, separated by module.

Codes are printed as a pair of module code + error code, e.g. `0006:0207`

Module codes are defined in `dcerror.h`.
https://github.com/DCMTK/dcmtk/blob/31ae87d57edf3f5a441cae64869c7337b29213b2/dcmdata/include/dcmtk/dcmdata/dcerror.h#L36-L75

Error codes are defined in multiple places within the DCMTK source, such as
https://github.com/DCMTK/dcmtk/blob/master/dcmnet/libsrc/cond.cc for networking errors.
"""

backoff_padding = 20
"""
If retrying timeouts with a backoff is on, this padding will be added to the existing timeout before retrying
"""

move_lock = threading.Lock()


class DcmtkDicomClient(BaseDicomClient):
    def __init__(
        self,
        client_ae,
        remote_ae,
        pacs_url,
        pacs_port,
        dicom_dir,
        dicom_tmp_dir=None,
        dcmtk_profile: str = "AllDICOM",
        timeout=20,
        storescp_extra_args=None,
        movescu_extra_args=None,
        findscu_extra_args=None,
        retry_timeouts_with_backoff=False,
        *args, **kwargs,
    ):
        """
        :param client_ae: Name for this client Association Entity. {client_ae}:11113
            needs to be registered with the remote PACS in order for C-MOVE to work
        :param pacs_url: Remote PACS URL
        :param pacs_port: Remote PACS port (usually 11112)
        :param dicom_dir: Root dir for storage of *.dcm files.
        :param dicom_tmp_dir: Root dir that stores temporary *.dcm files.
        :param dcmtk_profile: Profile name from storescp.cfg to use
        :param timeout: Connection and DICOM timeout in seconds
        :param storescp_extra_args: Optional array of extra arguments to supply to the `storescp` invocation
        :param findscu_extra_args: Optional array of extra arguments to supply to the `findscu` invocation
        :param movescu_extra_args: Optional array of extra arguments to supply to the `movescu` invocation
        :param retry_timeouts_with_backoff: If true, will retry failures due to timeout, with a longer timeout period.
            default=False

        Note: the `dcmtk_profile` variable refers to the profile name defined
        in the `storescp.cfg` configuration file, the location of which is
        indicated by the `SCPCFGPATH` environment variable. This configuration
        tells the `storescp` which presentation contexts to accept when
        negotiating an association with a storage SCU.

        "AllDICOM" is a profile name that exists in the default storescp
        configuration file which accepts almost all presentation contexts. If a
        more restricted set of contexts is desired, the configuration file
        should be updated and a new profile name should be passed in as an
        argument.

        For the use of `*_extra_args`: the array of arguments can be easily
        generated from a plain string with `shlex.split()`, e.g.

        >>> import shlex
        >>> extra_arguments = '--some-arg value --complex-string "multiple words here"'
        >>> storescp_extra_args = shlex.split(extra_arguments)
        >>> storescp_extra_args
        ['--some-arg', 'value', '--complex-string', 'multiple words here']
        """
        self.client_ae = client_ae
        self.remote_ae = remote_ae
        self.pacs_url = pacs_url
        self.pacs_port = str(pacs_port)
        self.dicom_dir = dicom_dir
        self.dicom_tmp_dir = dicom_tmp_dir if dicom_tmp_dir else os.path.join(self.dicom_dir, 'tmp')
        self.timeout = timeout
        self.listener_port = str(11113)
        self.storescp_extra_args = storescp_extra_args or []
        self.findscu_extra_args = findscu_extra_args or []
        self.movescu_extra_args = movescu_extra_args or []
        self.retry_timeouts_with_backoff = retry_timeouts_with_backoff
        self.dcmtk_profile = dcmtk_profile
        if logger.getEffectiveLevel() <= logging.DEBUG:
            self.logger_args = ['-v', '-d']
        else:
            self.logger_args = []

        # ensure binaries are available
        subprocess.run(['storescp', '--version'], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, check=True)
        subprocess.run(['movescu', '--version'], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, check=True)
        subprocess.run(['findscu', '--version'], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, check=True)

        # run 1 storescp listener at all times
        os.makedirs(self.dicom_tmp_dir, exist_ok=True)
        dcm_dict_dir = os.path.dirname(os.environ['DCMDICTPATH'])
        if 'SCPCFGPATH' in os.environ:
            storescp_config_path = os.environ['SCPCFGPATH']
        else:
            # fallback path typical to some dcmtk installations
            storescp_config_path = os.path.join(dcm_dict_dir, '../../etc/dcmtk/storescp.cfg')

        # TODO storescp logging is going to stdout: should have self.logger redirect
        storescp_args = ['storescp', '--fork', '--aetitle', client_ae,
                         *self.logger_args,
                         '--output-directory', self.dicom_tmp_dir,
                         '--filename-extension', '.dcm',
                         '--config-file', storescp_config_path, self.dcmtk_profile,
                         *self.storescp_extra_args,
                         self.listener_port]
        self.process = subprocess.Popen(storescp_args)

    def verify(self) -> bool:
        echoscu_args = ['echoscu', '--aetitle', self.remote_ae, '--call', self.client_ae,
                        *self._get_timeout_args(), self.pacs_url, self.pacs_port, *self.logger_args]

        result = subprocess.run(echoscu_args, stdout=PIPE, stderr=PIPE, universal_newlines=True)

        logger.debug(result.args)
        logger.debug(result.stdout)
        logger.debug(result.stderr)

        return result.returncode == 0

    def _get_timeout_args(self, is_retry=False):
        offset = backoff_padding if is_retry else 0
        return ['--timeout', str(self.timeout + offset),
                '--dimse-timeout', str(self.timeout + offset)]

    def _get_study_search_dataset(self, study_date_tag=None):
        search_dataset = Dataset()
        search_dataset.PatientID = None
        search_dataset.PatientName = ''
        search_dataset.PatientBirthDate = None
        if study_date_tag is not None:
            search_dataset.StudyDate = study_date_tag
        else:
            search_dataset.StudyDate = ''
        search_dataset.StudyInstanceUID = ''
        search_dataset.QueryRetrieveLevel = 'STUDY'
        return search_dataset

    def _send_c_find(self, search_dataset, is_retry=False):
        result_datasets = []

        search_dataset.is_little_endian = True
        search_dataset.is_implicit_VR = True
        with tempfile.TemporaryDirectory() as tmpdirname:
            find_dataset_path = os.path.join(tmpdirname, 'find_input.dcm')
            pydicom.dcmwrite(find_dataset_path, search_dataset)

            output_dir = os.path.join(tmpdirname, 'find_output')
            os.mkdir(output_dir)

            findscu_args = ['findscu', '--aetitle', self.client_ae, *self.logger_args,
                            '--call', self.remote_ae,
                            *self._get_timeout_args(is_retry), '-S',
                            '-X', '--output-directory', output_dir, *self.findscu_extra_args,
                            self.pacs_url, self.pacs_port, find_dataset_path]
            result = subprocess.run(findscu_args, stdout=PIPE, stderr=PIPE, universal_newlines=True)
            logger.debug(result.args)
            logger.debug(result.stdout)
            logger.debug(result.stderr)

            if result.returncode != 0:
                logger.error(
                    f'C-FIND failure for search dataset: rc {result.returncode}')
                logger.error(search_dataset)
                return []

            if _check_dcmtk_message_for_timeout(result.stdout or result.stderr):
                if self.retry_timeouts_with_backoff and not is_retry:
                    logger.warning('C-FIND timed out, but retry is on. Trying again.')
                    return self._send_c_find(search_dataset, True)
                logger.error('C-FIND failure for search dataset: Timed out.')
                logger.error(search_dataset)
                return []

            for dcm_file in glob.glob(f'{output_dir}/*.dcm'):
                result_datasets.append(dcmread(dcm_file))

        return result_datasets

    def _send_c_move(self, move_dataset, output_dir, is_retry=False):
        if self.process.returncode is not None:
            msg = 'dcmrecv is not running, rc {self.process.returncode}'
            logger.error(msg)
            raise Exception(msg)

        with tempfile.TemporaryDirectory() as tmpdirname:
            move_dataset_path = os.path.join(tmpdirname, 'move_dataset.dcm')

            os.makedirs(output_dir, exist_ok=True)
            move_dataset.is_little_endian = True
            move_dataset.is_implicit_VR = True
            pydicom.dcmwrite(move_dataset_path, move_dataset)

            # even though storescp has `--fork`, the move lock is needed to tell datasets
            #  apart in the `dicom_tmp_dir`
            with move_lock:
                movescu_args = ['movescu', '--aetitle', self.client_ae, '--call',
                                self.remote_ae,
                                '--move', self.client_ae, '-S',  # study query level
                                *self._get_timeout_args(), *self.logger_args, *self.movescu_extra_args,
                                self.pacs_url, self.pacs_port, move_dataset_path]
                result = subprocess.run(movescu_args, stdout=PIPE, stderr=PIPE, universal_newlines=True)

                logger.debug(result.args)
                logger.debug(result.stdout)
                logger.debug(result.stderr)

                for result_item in os.listdir(self.dicom_tmp_dir):
                    # fully specify move destination to allow overwrites
                    shutil.move(os.path.join(self.dicom_tmp_dir, result_item),
                                os.path.join(output_dir, result_item))

            if result.returncode != 0:
                logger.error(f'C-MOVE failure for query: rc {result.returncode}')
                return False

            if _check_dcmtk_message_for_timeout(result.stdout or result.stderr):
                if self.retry_timeouts_with_backoff and not is_retry:
                    logger.warning('C-MOVE timed out, but retry is on. Trying again.')
                    return self._send_c_move(move_dataset, output_dir, True)
                logger.error('C-MOVE failure for search dataset: Timed out.')
                return False

            return True

    def search_patients(self, search_query: Optional[str] = None,
                        search_query_type: Optional[str] = None,
                        additional_tags: Optional[List[str]] = None,
                        wildcard: bool = True) -> List[Dataset]:
        '''
        :param search_query: String containing query value to c_find.
            PatientID, PatientName, or, if wildcard is True, any partial there of.
        :param search_query_type: Optional string that restricts patient search to 'PatientID' or 'PatientName'.
            If None, perform a C-FIND for the query once on PatientID and again on PatientName.
        :param additional_tags: Additional dicom attributes that should be
            included in the returned patient dicom dataset values object.
        :param wildcard: Boolean stating whether to search based on
            any PatientName or PatientID partial string (i.e. Sam would find Samuel).
        :returns: List of DICOM query responses for each patient matching the query.
        '''
        if wildcard:
            search_query = f'*{search_query}*'
        patient_id_to_datasets = defaultdict(Dataset)

        search_dataset = self._get_study_search_dataset()
        if search_query_type == 'PatientID' or search_query_type is None:
            search_dataset.PatientID = search_query
            search_dataset.PatientName = ""
            self._search_patient_with_dataset(search_dataset, patient_id_to_datasets, additional_tags)
        if search_query_type == 'PatientName' or search_query_type is None:
            search_dataset.PatientID = ""
            search_dataset.PatientName = search_query
            self._search_patient_with_dataset(search_dataset, patient_id_to_datasets, additional_tags)

        return list(patient_id_to_datasets.values())

    def _search_patient_with_dataset(self, search_dataset: Dataset,
                                     patient_id_to_datasets: defaultdict,
                                     additional_tags: Optional[List[str]] = None):
        '''
        This function does not return any values but rather modifies the patient_id_to_datasets argument in-place.
        '''
        set_undefined_tags_to_blank(search_dataset, additional_tags)
        responses = self._send_c_find(search_dataset)
        for study in responses:
            if hasattr(study, 'PatientID'):
                result = patient_id_to_datasets[study.PatientID]
                self.update_patient_result(result, study, additional_tags)

    def studies_for_patient(self, patient_id, study_date_tag=None, additional_tags=None) -> List[Dataset]:
        search_dataset = self._get_study_search_dataset(study_date_tag)
        search_dataset.PatientID = patient_id
        set_undefined_tags_to_blank(search_dataset, additional_tags)

        responses = self._send_c_find(search_dataset)
        datasets = []
        for dataset in responses:
            # Some PACS send back empty "Success" responses at the end of the list
            if hasattr(dataset, 'PatientID'):
                datasets.append(dataset)

        return datasets

    def search_series(self, query_dataset, additional_tags=None) -> List[Dataset]:
        additional_tags = additional_tags or []
        query_dataset.QueryRetrieveLevel = 'SERIES'
        additional_tags += [
            'Modality',
            'SeriesDescription',
            'SeriesDate',
            'SeriesTime',
        ]
        set_undefined_tags_to_blank(query_dataset, additional_tags)

        datasets = []
        responses = self._send_c_find(query_dataset)
        for series in responses:
            if hasattr(series, 'SeriesInstanceUID'):
                datasets.append(series)
        return datasets

    def series_for_study(self, study_id, modality_filter=None, additional_tags=None,
                         manual_count=True) -> \
            List[Dataset]:
        additional_tags = additional_tags or []

        dataset = Dataset()
        dataset.StudyInstanceUID = study_id
        dataset.QueryRetrieveLevel = 'SERIES'

        additional_tags += [
            'SeriesInstanceUID',
            'BodyPartExamined',
            'SeriesDescription',
            'SeriesDate',
            'SeriesTime',
            'StudyDate',
            'StudyTime',
            'NumberOfSeriesRelatedInstances',
        ]
        set_undefined_tags_to_blank(dataset, additional_tags)
        # TODO modality filtering not implemented
        dataset.Modality = ''

        raw_series_datasets = self._send_c_find(dataset)

        series_datasets = []
        for series in raw_series_datasets:
            valid_dicom = hasattr(series, 'SeriesInstanceUID')
            modality = getattr(series, 'Modality', '')
            match = modality_filter is None or modality in modality_filter
            if valid_dicom and match:
                ds = Dataset()
                ds.SeriesDescription = getattr(series, 'SeriesDescription', '')
                ds.BodyPartExamined = getattr(series, 'BodyPartExamined', None)
                ds.SeriesInstanceUID = series.SeriesInstanceUID
                ds.Modality = series.Modality
                copy_dicom_attributes(ds, series, additional_tags)
                ds.NumberOfSeriesRelatedInstances = self._determine_number_of_images(
                    series, manual_count)
                series_datasets.append(ds)

        return series_datasets

    def _determine_number_of_images(self, series, manual_count):
        answer_from_instance_count = series.NumberOfSeriesRelatedInstances
        if answer_from_instance_count:
            return answer_from_instance_count
        elif manual_count:
            return str(self._count_images_via_query(series))
        else:
            return None

    def _count_images_via_query(self, series):
        series_dataset = Dataset()
        series_dataset.SeriesInstanceUID = series.SeriesInstanceUID
        series_dataset.QueryRetrieveLevel = 'IMAGE'
        series_dataset.SOPInstanceUID = ''

        series_responses = self._send_c_find(series_dataset)
        image_count = 0
        for instance in series_responses:
            if hasattr(instance, 'SOPInstanceUID'):
                image_count += 1
        return image_count

    def images_for_series(self, study_id, series_id, additional_tags=None,
                          max_count=None) -> List[Dataset]:

        image_datasets = []

        series_dataset = Dataset()
        series_dataset.StudyInstanceUID = study_id
        series_dataset.SeriesInstanceUID = series_id
        series_dataset.QueryRetrieveLevel = 'IMAGE'
        series_dataset.SOPInstanceUID = ''
        set_undefined_tags_to_blank(series_dataset, additional_tags)

        series_responses = self._send_c_find(series_dataset)
        for instance in series_responses:
            if hasattr(instance, 'SOPInstanceUID'):
                ds = Dataset()
                ds.SeriesInstanceUID = instance.SeriesInstanceUID
                ds.SOPInstanceUID = instance.SOPInstanceUID
                copy_dicom_attributes(ds, instance, additional_tags)
                image_datasets.append(ds)
                if max_count and len(image_datasets) >= max_count:
                    break
        return image_datasets

    def fetch_images_as_dicom_files(self, study_id: str, series_id: str) -> Optional[str]:
        series_path = os.path.join(self.dicom_dir, series_id)

        dataset = Dataset()
        dataset.SeriesInstanceUID = series_id
        dataset.StudyInstanceUID = study_id
        dataset.QueryRetrieveLevel = 'SERIES'
        dataset.SOPInstanceUID = ''

        success = self._send_c_move(dataset, series_path)

        return series_path if success and os.path.exists(series_path) else None

    def fetch_image_as_dicom_file(self, study_id: str, series_id: str,
                                  sop_instance_id: str) -> Optional[str]:
        series_path = os.path.join(self.dicom_dir, series_id)
        dataset = Dataset()
        dataset.SeriesInstanceUID = series_id
        dataset.StudyInstanceUID = study_id
        dataset.SOPInstanceUID = sop_instance_id
        dataset.QueryRetrieveLevel = 'IMAGE'

        success = self._send_c_move(dataset, self.series_path)
        filepath = os.path.join(series_path, dicom_filename(dataset))

        return filepath if success and os.path.exists(filepath) else None

    def fetch_thumbnail(self, study_id: str, series_id: str) -> Optional[str]:
        # search for image IDs in the series
        find_dataset = Dataset()
        find_dataset.StudyInstanceUID = study_id
        find_dataset.SeriesInstanceUID = series_id
        find_dataset.QueryRetrieveLevel = 'IMAGE'
        find_dataset.SOPInstanceUID = ''
        image_responses = self._send_c_find(find_dataset)

        image_ids = []
        for dataset in image_responses:
            if hasattr(dataset, 'SOPInstanceUID'):
                image_ids.append(dataset.SOPInstanceUID)

        if not image_ids:
            logger.error(f'Failed to find any image instances for series {series_id}')
            return None

        # try to get the middle image in the series for the thumbnail:
        #  instance ID order is usually the same as slice order but not guaranteed
        #  by the standard.
        middle_image_id = image_ids[len(image_ids) // 2]
        return self._fetch_individual_slice_thumbnail(study_id, series_id, middle_image_id)

    def fetch_slice_thumbnail(self, study_id: str, series_id: str,
                              instance_id: str) -> Optional[str]:
        return self._fetch_individual_slice_thumbnail(study_id, series_id, instance_id)

    def _fetch_individual_slice_thumbnail(self, study_id: str, series_id: str,
                                          instance_id: str) -> Optional[str]:
        move_dataset = Dataset()
        move_dataset.StudyInstanceUID = study_id
        move_dataset.SeriesInstanceUID = series_id
        move_dataset.SOPInstanceUID = instance_id
        move_dataset.QueryRetrieveLevel = 'IMAGE'

        success = self._send_c_move(move_dataset, self.dicom_dir)

        # dcmtk puts modality prefixes in front of the instance IDs
        dcm_paths = glob.glob(os.path.join(self.dicom_dir, f'*{instance_id}.dcm'))
        if not success or not dcm_paths:
            logger.error(f'Failure to get thumbnail for {instance_id}')
            return None
        if len(dcm_paths) > 1:
            logger.error(f'Found duplicate thumbnails for {instance_id}: {dcm_paths}')
            return None

        dcm_path = dcm_paths[0]
        png_path = process_and_write_png_from_file(dcm_path)
        return png_path

    def send_datasets(self, datasets: Iterable[Dataset], override_remote_ae: str = None,
                      override_pacs_url: str = None, override_pacs_port: int = None) -> None:

        if override_remote_ae is not None and override_pacs_url is not None and override_pacs_port is not None:
            send_remote_ae = override_remote_ae
            send_port = str(override_pacs_port)
            send_url = override_pacs_url
        else:
            send_remote_ae = self.remote_ae
            send_port = self.pacs_port
            send_url = self.pacs_url

        for dataset in datasets:
            logger.info('Sending %s', dataset.SeriesInstanceUID)
            with tempfile.TemporaryDirectory() as tmpdirname:
                store_dcm_file = os.path.join(tmpdirname, 'store_dataset.dcm')
                pydicom.dcmwrite(store_dcm_file, dataset)
                storescu_args = ['storescu', '--aetitle', self.client_ae,
                                 '--call', send_remote_ae,
                                 *self._get_timeout_args(), *self.logger_args,
                                 send_url, send_port,
                                 store_dcm_file]

                result = subprocess.run(storescu_args, stdout=PIPE, stderr=PIPE,
                                        universal_newlines=True)
                logger.debug(result.args)
                logger.debug(result.stdout)
                logger.debug(result.stderr)
                if result.returncode != 0:
                    msg = f'Failure to send dataset with {dataset.SeriesInstanceUID}, rc {result.returncode}'
                    logger.error(msg)
                    raise Exception(msg)


def _check_dcmtk_message_for_error(dcmtk_message: str) -> Optional[Tuple[int, int]]:
    """
    This checks a message from DCMTK for a known error message pattern.

    :param dcmtk_message: The message to parse / check for an error message.
    :return: Either `None` if no error is detected, else a tuple with (module_code, err_code)

    This is useful, because for certain operations, DCMTK is likely to always return a zero exit code, even in cases
    of (nested) failure.

    For example, `findscu` calls will seemingly only return a non-zero exit code if the failure happens at the
    association level (e.g. on setup or abort).

    This is a known issue: https://support.dcmtk.org/redmine/issues/929
    """
    # Standard pattern is E: 0001:0002 ERROR MESSAGE
    pattern = re.compile(r"[EF]: ([\da-f]{4}:[\da-f]{4}) [^#\r\n]+$", flags=re.MULTILINE)

    # Only check last three lines, and in reverse order (last first)
    message_lines = dcmtk_message.splitlines()[-3:]
    message_lines.reverse()
    for line in message_lines:
        match = pattern.search(line)
        if match:
            return tuple(map(lambda code: int(code, 16), match.group(1).split(':')))

    return None


def _check_dcmtk_message_for_timeout(dcmtk_message: str) -> bool:
    error_tuple = _check_dcmtk_message_for_error(dcmtk_message)
    return error_tuple == dcmtk_error_codes['dcmnet-DIMSEC_NODATAAVAILABLE']
