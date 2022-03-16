'''
This filesystem client can be used for testing in development when a PACS server
is not available. It may be slow if many datasets are present: All get/fetch operations
are O(N) on the number of DICOM datasets loaded from the `test_dicom_data` dir.

Example data located in `test_dicom_data` dir:
 (from www.dicomserver.co.uk).

Patient ID PAT001 "Joe Bloggss" dob 19450703
    Study ID 1.2.826.0.1.3680043.11.1011
        Study Date 20180522
        CT Modality
            Series ID 1.2.826.0.1.3680043.6.86796.74495.20180522152336.14136.1.23
                1 Image

Patient ID PAT014 "Erica Richardson" dob 19520314
    Study ID 1.2.826.0.1.3680043.11.118
        Study Date 20180518
        CT Modality
            Series ID 1.2.826.0.1.3680043.6.51581.36765.20180518132103.25992.1.21
                5 images
    Study ID 1.2.826.0.1.3680043.11.118.1
'''
import glob
import logging
import os
import shutil
from datetime import datetime
from collections import defaultdict
from typing import List, Optional, Dict, Iterable

from pydicom import dcmread, Dataset
from pydicom.valuerep import MultiValue
from pydicom.uid import UID

from .base_client import BaseDicomClient, PRIVATE_ID
from .utils import process_and_write_png_from_file, copy_dicom_attributes, dicom_filename

logger = logging.getLogger(__name__)


class FilesystemDicomClient(BaseDicomClient):
    def __init__(self, dicom_dir: str, dicom_source_dir: str, *args, **kwargs) -> None:
        """
        :param dicom_src_dir: source directory for *.dcm files
        :param dicom_dir: the DICOM output dir for image retrievals (same as other clients)
        """
        self.dicom_dir = dicom_dir
        os.makedirs(self.dicom_dir, exist_ok=True)
        self.dicom_source_dir = dicom_source_dir

        self.dicom_datasets: Dict[str, Dataset] = {}

        for dicom_file in glob.glob(f'{dicom_source_dir}/**/*.dcm', recursive=True):
            self._read_and_add_data_set(dicom_file)

    def _read_and_add_data_set(self, filename: str) -> None:
        filepath = self._filepath(filename)
        self._add_dataset(dcmread(filepath, stop_before_pixels=True), filepath)

    def _add_dataset(self, dataset: Dataset, filepath: str = None) -> None:
        if filepath is None:
            filepath = self._filepath(dicom_filename(dataset))
        self.dicom_datasets[filepath] = dataset

    def _filepath(self, filename):
        return os.path.join(self.dicom_source_dir, filename)

    def verify(self) -> bool:
        return True

    def search_patients(self, search_query: str, additional_tags: List[str] = None,
                        wildcard: bool = True) -> List[Dataset]:
        patient_id_to_results = defaultdict(Dataset)

        # Build patient-level datasets from the instance-level test data
        for dataset in self.dicom_datasets.values():
            patient_id = getattr(dataset, 'PatientID', '').lower()
            patient_name = str(getattr(dataset, 'PatientName', '')).lower()
            search_query = search_query.lower()
            if wildcard:
                match = (search_query in patient_id) or (search_query in patient_name)
            else:
                match = (search_query == patient_id) or (search_query == patient_name)
            if match:
                result = patient_id_to_results[patient_id]
                self.update_patient_result(result, dataset, additional_tags)
        return list(patient_id_to_results.values())

    def search_series(self, query_dataset, additional_tags=None) -> List[Dataset]:
        # Build series-level datasets from the instance-level test data
        additional_tags = additional_tags or []
        result_datasets = []
        for dataset in self.dicom_datasets.values():
            series_matches = dataset.SeriesInstanceUID == query_dataset.SeriesInstanceUID
            if series_matches:
                ds = Dataset()
                additional_tags += [
                    'PatientName',
                    'PatientBirthDate',
                    'BodyPartExamined',
                    'SeriesDescription',
                    'PatientPosition',
                ]
                ds.PatientStudyInstanceUIDs = MultiValue(UID, [dataset.StudyInstanceUID])
                ds.PacsmanPrivateIdentifier = PRIVATE_ID
                ds.PatientMostRecentStudyDate = getattr(dataset, 'StudyDate', '')
                copy_dicom_attributes(ds, dataset, additional_tags)
                result_datasets.append(ds)
        return result_datasets

    def studies_for_patient(self, patient_id, study_date_tag=None, additional_tags=None) -> List[Dataset]:
        # additional tags are ignored here; only tags available are already in the files
        study_id_to_dataset: Dict[str, Dataset] = {}

        date_format_str = '%Y%m%d'  # e.g. 20210101
        study_start_date = study_end_date = None
        if study_date_tag is not None:
            study_start_str, study_end_str = study_date_tag.split('-')
            study_start_date = datetime.strptime(study_start_str, date_format_str).date()
            study_end_date = datetime.strptime(study_end_str, date_format_str).date()

        def date_filter(study_ds):
            if hasattr(study_ds, 'StudyDate'):
                study_date_str = dataset.StudyDate
            elif hasattr(study_ds, 'SeriesDate'):
                study_date_str = dataset.SeriesDate
            else:
                study_date_str = None

            if study_start_date is None or study_end_date is None or study_date_str is None:
                return True
            study_date = datetime.strptime(study_date_str, date_format_str).date()
            return study_date >= study_start_date and study_date <= study_end_date

        # Return one dataset per study
        for dataset in self.dicom_datasets.values():
            if patient_id == dataset.PatientID and dataset.StudyInstanceUID not in study_id_to_dataset:
                if date_filter(dataset):
                    study_id_to_dataset[dataset.StudyInstanceUID] = dataset
        return list(study_id_to_dataset.values())

    def series_for_study(self, study_id, modality_filter=None, additional_tags=None,
                         manual_count=True) -> List[Dataset]:
        # Build series-level datasets from the instance-level test data
        series_id_to_dataset: Dict[str, Dataset] = {}
        for dataset in self.dicom_datasets.values():
            study_matches = dataset.StudyInstanceUID == study_id
            modality_matches = modality_filter is None or getattr(dataset, 'Modality', '') in modality_filter
            if study_matches and modality_matches:
                dataset.PacsmanPrivateIdentifier = PRIVATE_ID
                dataset.BodyPartExamined = getattr(dataset, 'BodyPartExamined', '')
                dataset.SeriesDescription = getattr(dataset, 'SeriesDescription', '')
                dataset.PatientPosition = getattr(dataset, 'PatientPosition', '')
                series_id = dataset.SeriesInstanceUID
                if series_id in series_id_to_dataset:
                    series_id_to_dataset[series_id].NumberOfSeriesRelatedInstances += 1
                else:
                    dataset.NumberOfSeriesRelatedInstances = 1
                    series_id_to_dataset[series_id] = dataset

        return list(series_id_to_dataset.values())

    def images_for_series(self, study_id, series_id, additional_tags=None, max_count=None) -> List[Dataset]:
        image_datasets = []
        for dataset in self.dicom_datasets.values():
            series_matches = dataset.SeriesInstanceUID == series_id and dataset.StudyInstanceUID == study_id
            if series_matches:
                image_datasets.append(dataset)
            if max_count and len(image_datasets) >= max_count:
                break
        return image_datasets

    def fetch_images_as_dicom_files(self, study_id: str, series_id: str) -> Optional[str]:
        result_dir = os.path.join(self.dicom_dir, series_id)
        os.makedirs(result_dir, exist_ok=True)
        found = False
        for (path, ds) in self.dicom_datasets.items():
            if ds.SeriesInstanceUID == series_id:
                found = True
                dest_path = os.path.join(result_dir, f'{ds.SOPInstanceUID}.dcm')
                shutil.copy(path, dest_path)
        if found:
            return result_dir
        else:
            return None

    def fetch_image_as_dicom_file(self, study_id: str, series_id: str, sop_instance_id: str) -> Optional[str]:
        result_dir = os.path.join(self.dicom_dir, series_id)
        os.makedirs(result_dir, exist_ok=True)
        for (path, ds) in self.dicom_datasets.items():
            if ds.SOPInstanceUID == sop_instance_id:
                return shutil.copy(path, os.path.join(result_dir))
        return None

    def fetch_thumbnail(self, study_id: str, series_id: str) -> Optional[str]:
        series_items = []
        for path_to_ds in self.dicom_datasets.items():
            if path_to_ds[1].SeriesInstanceUID == series_id:
                series_items.append(path_to_ds)
        if not series_items:
            return None

        series_items = sorted(series_items, key=lambda t: t[1].SOPInstanceUID)

        thumbnail_index = len(series_items) // 2
        thumbnail_archive_path = series_items[thumbnail_index][0]
        thumbnail_instance_id = series_items[thumbnail_index][1].SOPInstanceUID

        # copying to instance ID ensures that the filename is unique
        dcm_path = os.path.join(self.dicom_dir, f'{thumbnail_instance_id}.dcm')
        shutil.copy(thumbnail_archive_path, dcm_path)

        png_path = process_and_write_png_from_file(dcm_path)
        return png_path

    def fetch_slice_thumbnail(self, study_id: str, series_id: str,
                              instance_id: str) -> Optional[str]:
        for path, ds in self.dicom_datasets.items():
            if ds.SeriesInstanceUID == series_id and ds.SOPInstanceUID == instance_id:
                thumbnail_series_path = path
                dcm_path = os.path.join(self.dicom_dir, f'{instance_id}.dcm')
                shutil.copy(thumbnail_series_path, dcm_path)
                png_path = process_and_write_png_from_file(dcm_path)
                return png_path
        logger.warning(f'Could not find instance {instance_id} for series {series_id}')
        return None

    def send_datasets(self, datasets: Iterable[Dataset], override_remote_ae: str = None,
                      override_pacs_url: str = None, override_pacs_port: int = None) -> None:
        new_dicom_datasets = {}
        for dataset in datasets:
            filepath = self._filepath(dicom_filename(dataset))
            new_dicom_datasets[filepath] = dataset
        self.dicom_datasets = {**self.dicom_datasets, **new_dicom_datasets}
