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
from collections import defaultdict

from pydicom import dcmread, Dataset
from pydicom.valuerep import MultiValue

from .dicom_interface import DicomInterface, PRIVATE_ID
from .utils import process_and_write_png, copy_dicom_attributes


logger = logging.getLogger(__name__)


class FilesystemDicomClient(DicomInterface):
    def __init__(self, dicom_dir, dicom_source_dir, *args, **kwargs):
        """
        :param dicom_src_dir: source directory for *.dcm files
        :param dicom_dir: the DICOM output dir for image retrievals (same as other clients)
        """
        self.dicom_dir = dicom_dir
        os.makedirs(self.dicom_dir, exist_ok=True)
        self.dicom_datasets = {}
        for dicom_file in glob.glob(f'{dicom_source_dir}/**/*.dcm', recursive=True):
            filepath = os.path.join(dicom_source_dir, dicom_file)
            self.dicom_datasets[filepath] = dcmread(filepath)

    def verify(self):
        return True

    def search_patients(self, search_query, additional_tags=None):
        patient_id_to_results = defaultdict(Dataset)

        # support the * wildcard with "in string" test for each dataset
        search_query = search_query.replace('*', '')

        # Build patient-level datasets from the instance-level test data
        for dataset in self.dicom_datasets.values():
            patient_id = getattr(dataset, 'PatientID', '')
            patient_name = getattr(dataset, 'PatientName', '')
            if (search_query in patient_id) or (search_query in patient_name):
                result = patient_id_to_results[patient_id]
                self.update_patient_result(result, dataset)
        return list(patient_id_to_results.values())

    def search_series(self, query_dataset, additional_tags=None):
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
                ds.PatientStudyIDs = MultiValue(str, [dataset.StudyInstanceUID])
                ds.PacsmanPrivateIdentifier = PRIVATE_ID
                ds.PatientMostRecentStudyDate = dataset.StudyDate
                copy_dicom_attributes(ds, dataset, additional_tags)
                result_datasets.append(ds)
        return result_datasets

    def studies_for_patient(self, patient_id, additional_tags=None):
        # additional tags are ignored here; only tags available are already in the files
        study_id_to_dataset = {}

        # Return one dataset per study
        for dataset in self.dicom_datasets.values():
            if patient_id == dataset.PatientID and dataset.StudyInstanceUID not in study_id_to_dataset:
                study_id_to_dataset[dataset.StudyInstanceUID] = dataset
        return list(study_id_to_dataset.values())

    def series_for_study(self, study_id, modality_filter=None, additional_tags=None):
        # Build series-level datasets from the instance-level test data
        series_id_to_dataset = {}
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

    def images_for_series(self, series_id, additional_tags=None, max_count=None):
        image_datasets = []
        for dataset in self.dicom_datasets.values():
            series_matches = dataset.SeriesInstanceUID == series_id
            if series_matches:
                image_datasets.append(dataset)
            if max_count and len(image_datasets) >= max_count:
                break
        return image_datasets

    def fetch_images_as_dicom_files(self, series_id):
        result_dir = os.path.join(self.dicom_dir, series_id)
        os.makedirs(result_dir, exist_ok=True)
        found = False
        for (path, ds) in self.dicom_datasets.items():
            if ds.SeriesInstanceUID == series_id:
                found = True
                shutil.copy(path, os.path.join(result_dir))
        if found:
            return result_dir
        else:
            return None

    def fetch_image_as_dicom_file(self, series_id, sop_instance_id):
        result_dir = os.path.join(self.dicom_dir, series_id)
        os.makedirs(result_dir, exist_ok=True)
        for (path, ds) in self.dicom_datasets.items():
            if ds.SOPInstanceUID == sop_instance_id:
                return shutil.copy(path, os.path.join(result_dir))
        return None

    def fetch_thumbnail(self, series_id):
        series_items = []
        for path_to_ds in self.dicom_datasets.items():
            if path_to_ds[1].SeriesInstanceUID == series_id:
                series_items.append(path_to_ds)
        if not series_items:
            return None

        series_items = sorted(series_items, key=lambda t: t[1].SOPInstanceUID)

        thumbnail_series_path = series_items[len(series_items) // 2][0]
        shutil.copy(thumbnail_series_path, self.dicom_dir)

        thumbnail_filename = os.path.basename(thumbnail_series_path)
        dcm_path = os.path.join(self.dicom_dir, thumbnail_filename)
        try:
            thumbnail_ds = dcmread(dcm_path)
            png_path = os.path.splitext(dcm_path)[0] + '.png'
            process_and_write_png(thumbnail_ds, png_path)
        finally:
            os.remove(dcm_path)
        return png_path
