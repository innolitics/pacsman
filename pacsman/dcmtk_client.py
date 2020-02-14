import logging
import os
import subprocess
import shutil
import tempfile
import threading
import glob
from collections import defaultdict

from typing import List, Optional, Iterable

import pydicom
from pydicom import dcmread
from pydicom.dataset import Dataset

from .base_client import BaseDicomClient
from .utils import process_and_write_png, copy_dicom_attributes, \
    set_undefined_tags_to_blank, dicom_filename

logger = logging.getLogger(__name__)

# http://dicom.nema.org/medical/dicom/current/output/html/part07.html#chapter_C
status_success_or_pending = [0x0000, 0xFF00, 0xFF01]

socket_lock = threading.Lock()


class DcmtkDicomClient(BaseDicomClient):
    def __init__(self, client_ae, remote_ae, pacs_url, pacs_port, dicom_dir, timeout=20,
                 *args, **kwargs):
        """
        :param client_ae: Name for this client Association Entity. {client_ae}:11113
            needs to be registered with the remote PACS in order for C-MOVE to work
        :param pacs_url: Remote PACS URL
        :param pacs_port: Remote PACS port (usually 11112)
        :param dicom_dir: Root dir for storage of *.dcm files.
        :param timeout: Connection and DICOM timeout in seconds
        """
        self.client_ae = client_ae
        self.remote_ae = remote_ae
        self.pacs_url = pacs_url
        self.pacs_port = str(pacs_port)
        self.dicom_dir = dicom_dir
        self.dicom_tmp_dir = os.path.join(self.dicom_dir, 'tmp')
        self.timeout = timeout
        self.listener_port = str(11113)
        self.timeout_args = ['--timeout', str(self.timeout),
                             '--dimse-timeout', str(self.timeout)]

        # ensure binaries are available
        subprocess.run(['dcmrecv', '-v'])
        subprocess.run(['movescu', '-v'])
        subprocess.run(['findscu', '-v'])

        os.makedirs(self.dicom_tmp_dir, exist_ok=True)
        dcm_dict_dir = os.path.dirname(os.environ['DCMDICTPATH'])
        if 'SCPCFGPATH' in os.environ:
            storescp_config_path = os.environ['SCPCFGPATH']
        else:
            storescp_config_path = os.path.join(dcm_dict_dir, '../../etc/dcmtk/storescp.cfg')

        dcmrecv_args = ['dcmrecv', self.listener_port, '--aetitle', client_ae,
                             '--output-directory', self.dicom_tmp_dir,
                             '--filename-extension', '.dcm',
                             '--config-file', storescp_config_path, 'AllDICOM']
        self.process = None
        self.process = subprocess.Popen(dcmrecv_args)

    def verify(self) -> bool:
        echoscu_args = ['echoscu', '--aetitle', self.remote_ae, '--call', self.client_ae,
                        *self.timeout_args, self.pacs_url, self.pacs_port]

        result = subprocess.run(echoscu_args)

        logger.debug(result.args)
        logger.debug(result.stdout)
        logger.debug(result.stderr)

        return result.returncode == 0

    def _get_study_search_dataset(self):
        search_dataset = Dataset()
        search_dataset.PatientID = None
        search_dataset.PatientName = ''
        search_dataset.PatientBirthDate = None
        search_dataset.StudyDate = ''
        search_dataset.StudyInstanceUID = ''
        search_dataset.QueryRetrieveLevel = 'STUDY'
        return search_dataset

    def _send_c_find(self, search_dataset):
        result_datasets = []

        search_dataset.is_little_endian = True
        with tempfile.TemporaryDirectory() as tmpdirname:
            find_dataset_path = os.path.join(tmpdirname, 'find_input.dcm')
            pydicom.dcmwrite(find_dataset_path, search_dataset)

            output_dir = os.path.join(tmpdirname, 'find_output')
            os.mkdir(output_dir)

            findscu_args = ['findscu', '--aetitle', self.client_ae, '-d', '-v', '--call',
                            self.remote_ae,
                            *self.timeout_args, '-S',
                                                '-X', '--output-directory', output_dir,
                            self.pacs_url, self.pacs_port, find_dataset_path]
            result = subprocess.run(findscu_args)
            logger.debug(result.args)
            logger.debug(result.stdout)
            logger.debug(result.stderr)

            if result.returncode != 0:
                logger.error(
                    f'C-FIND failure for search dataset: rc {result.returncode}')
                logger.error(search_dataset)
                return []

            for dcm_file in glob.glob(f'{output_dir}/*.dcm'):
                result_datasets.append(dcmread(dcm_file))

        return result_datasets

    def _send_c_move(self, move_dataset, output_dir):
        if self.process.returncode is not None:
            msg = 'dcmrecv is not running, rc {self.process.returncode}'
            logger.error(msg)
            raise Exception(msg)

        socket_lock.acquire()
        with tempfile.TemporaryDirectory() as tmpdirname:
            move_dataset_path = os.path.join(tmpdirname, 'move_dataset.dcm')

            os.makedirs(output_dir, exist_ok=True)

            pydicom.dcmwrite(move_dataset_path, move_dataset)
            movescu_args = ['movescu', '--aetitle', self.client_ae, '--call',
                            self.remote_ae,
                            '--move', self.client_ae,
                            *self.timeout_args, '-S',
                            self.pacs_url, self.pacs_port, move_dataset_path]
            result = subprocess.run(movescu_args)

            logger.debug(result.args)
            logger.debug(result.stdout)
            logger.debug(result.stderr)

            for result_item in os.listdir(self.dicom_tmp_dir): 
                shutil.move(os.path.join(self.dicom_tmp_dir, result_item), os.path.join(output_dir, result_item))

            socket_lock.release()
            if result.returncode != 0:
                logger.error(f'C-MOVE failure for query: rc {result.returncode}')
                return False
            return True

    def search_patients(self, search_query: str, additional_tags: List[str] = None) -> \
    List[Dataset]:
        search_query = f'*{search_query}*'
        patient_id_to_datasets = defaultdict(Dataset)

        # first search on the patient ID field
        search_dataset = self._get_study_search_dataset()
        search_dataset.PatientID = search_query
        set_undefined_tags_to_blank(search_dataset, additional_tags)

        id_responses = self._send_c_find(search_dataset)
        for study in id_responses:
            if hasattr(study, 'PatientID'):
                result = patient_id_to_datasets[study.PatientID]
                self.update_patient_result(result, study, additional_tags)

        # then search with the same query on the patient name field
        search_dataset = self._get_study_search_dataset()
        search_dataset.PatientName = search_query
        set_undefined_tags_to_blank(search_dataset, additional_tags)

        name_responses = self._send_c_find(search_dataset)
        for study in name_responses:
            if hasattr(study, 'PatientID'):
                result = patient_id_to_datasets[study.PatientID]
                self.update_patient_result(result, study, additional_tags)

        return list(patient_id_to_datasets.values())

    def studies_for_patient(self, patient_id, additional_tags=None) -> List[Dataset]:
        search_dataset = self._get_study_search_dataset()
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
        query_dataset.QueryRetrieveLevel = 'IMAGE'
        additional_tags += [
            'Modality',
            'BodyPartExamined',
            'SeriesDescription',
            'SeriesDate',
            'SeriesTime',
            'PatientPosition',
        ]
        set_undefined_tags_to_blank(query_dataset, additional_tags)

        datasets = []
        responses = self._send_c_find(query_dataset)
        for series in responses:
            if hasattr(series, 'SeriesInstanceUID'):
                datasets.append(series)
        return datasets

    def series_for_study(self, study_id, modality_filter=None, additional_tags=None) -> \
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
            'PatientPosition',
            'NumberOfSeriesRelatedInstances',
        ]
        set_undefined_tags_to_blank(dataset, additional_tags)
        # TODO check if filtering modality with 'MR\\CT' works in dcmtk
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
                ds.SeriesDate = series.SeriesDate
                ds.SeriesTime = series.SeriesTime
                copy_dicom_attributes(ds, series, additional_tags)
                ds.NumberOfSeriesRelatedInstances = self._determine_number_of_images(
                    series)
                series_datasets.append(ds)

        return series_datasets

    def _determine_number_of_images(self, series):
        answer_from_instance_count = series.NumberOfSeriesRelatedInstances
        if answer_from_instance_count:
            return answer_from_instance_count
        else:
            return str(self._count_images_via_query(series))

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

        #with StorageSCP(self.client_ae, series_path) as scp:
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

        #with StorageSCP(self.client_ae, series_path) as scp:
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
            return None

        #with StorageSCP(self.client_ae, self.dicom_dir) as scp:
        # try to get the middle image in the series for the thumbnail:
        #  instance ID order is usually the same as slice order but not guaranteed
        #  by the standard.
        middle_image_id = image_ids[len(image_ids) // 2]
        move_dataset = Dataset()
        move_dataset.StudyInstanceUID = study_id
        move_dataset.SeriesInstanceUID = series_id
        move_dataset.SOPInstanceUID = middle_image_id
        move_dataset.QueryRetrieveLevel = 'IMAGE'

        success = self._send_c_move(move_dataset, self.dicom_dir)

        # dcmtk puts modality prefixes in front of the instance IDs
        dcm_paths = glob.glob(os.path.join(self.dicom_dir, f'*{middle_image_id}.dcm'))
        if not success or not dcm_paths:
            logger.error(f'Failure to get thumbnail for {middle_image_id}')
            return None
        if len(dcm_paths) > 1:
            logger.error(f'Found duplicate thumbnails for {middle_image_id}: {dcm_paths}')
            return None

        dcm_path = dcm_paths[0]

        try:
            thumbnail_ds = dcmread(dcm_path)
            png_path = os.path.splitext(dcm_path)[0] + '.png'
            process_and_write_png(thumbnail_ds, png_path)
        finally:
            os.remove(dcm_path)
        return png_path

    def send_datasets(self, datasets: Iterable[Dataset]) -> None:
        """
        Send dicom datasets
        :param datasets:
        :return:
        """
        for dataset in datasets:
            logger.info('Sending %s', dataset.SeriesInstanceUID)
            with tempfile.TemporaryDirectory() as tmpdirname:
                store_dcm_file = os.path.join(tmpdirname, 'store_dataset.dcm')
                pydicom.dcmwrite(store_dcm_file, dataset)
                storescu_args = ['storescu', '--aetitle', self.client_ae,
                                 '--call', self.remote_ae, *self.timeout_args,
                                 self.pacs_url, self.pacs_port,
                                 store_dcm_file]

                subprocess.run(['cp', store_dcm_file, '/Users/dillon/innolitics/pacsman/pacsman'])
                result = subprocess.run(storescu_args)
                logger.debug(result.args)
                logger.debug(result.stdout)
                logger.debug(result.stderr)
                if result.returncode != 0:
                    logger.error(
                        f'Failure to send dataset with {dataset.SeriesInstanceUID}')


# TODO this is currently being handled by the movescu listener
'''
class StorageSCP():
    def __init__(self, client_ae, result_dir):
        listener_port = str(11113)

        dcm_dict_dir = os.path.dirname(os.environ['DCMDICTPATH'])
        storescp_config_path = os.path.join(dcm_dict_dir, '../../etc/dcmtk/storescp.cfg')
        self.dcmrecv_args = ['dcmrecv', listener_port, '--aetitle', client_ae,
                             '--output-directory', result_dir,
                             '--filename-extension', '.dcm',
                             '--config-file', storescp_config_path, 'AllDICOM']
        self.process = None

    def __enter__(self):
        socket_lock.acquire()
        self.process = subprocess.Popen(self.dcmrecv_args)
        return self.process

    def __exit__(self, exct_type, exce_value, traceback):
        if self.process:
            self.process.kill()
            stdout, stderr = self.process.communicate()
            logger.debug(self.dcmrecv_args)
            logger.debug(stdout)
            logger.debug(stderr)
        socket_lock.release()
'''

