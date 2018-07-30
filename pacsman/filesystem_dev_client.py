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

from pydicom import dcmread, Dataset
from pydicom.valuerep import MultiValue

from .dicom_interface import DicomInterface
from .utils import process_and_write_png

logger = logging.getLogger(__name__)


file_dir = os.path.dirname(os.path.abspath(__file__))
dicom_source_dir = os.path.join(file_dir, 'test_dicom_data')


class FilesystemDicomClient(DicomInterface):

    def __init__(self, client_ae, pacs_url, pacs_port, dicom_dir, timeout=5):
        logger.debug(f'Ignoring dummy parameters: {client_ae}, {pacs_url}:{pacs_port}, \
                    timeout {timeout}s')

        # this is the DICOM output dir for image retrievals
        self.dicom_dir = dicom_dir

        self.dicom_datasets = {}

        for dicom_file in glob.glob(f'{dicom_source_dir}/*.dcm'):
            filepath = os.path.join(dicom_source_dir, dicom_file)
            self.dicom_datasets[filepath] = dcmread(filepath)

    def verify(self):
        return True

    def search_patients(self, search_query, additional_tags=None):
        patient_id_to_datasets = {}
        # Build patient-level datasets from the instance-level test data
        for dataset in self.dicom_datasets.values():
            if search_query in dataset.PatientID or search_query in str(dataset.PatientName):
                patient_id = dataset.PatientID
                if patient_id in patient_id_to_datasets:
                    if dataset.StudyDate > patient_id_to_datasets[patient_id].PatientMostRecentStudyDate:
                        patient_id_to_datasets[patient_id].PatientMostRecentStudyDate = dataset.StudyDate

                    patient_id_to_datasets[patient_id].PatientStudyIDs.append(
                        dataset.StudyInstanceUID)
                else:
                    ds = Dataset()
                    ds.PatientID = patient_id
                    ds.PatientName = dataset.PatientName
                    ds.PatientBirthDate = dataset.PatientBirthDate
                    ds.PatientStudyIDs = MultiValue(str, [dataset.StudyInstanceUID])

                    ds.PacsmanPrivateIdentifier = 'pacsman'
                    ds.PatientMostRecentStudyDate = dataset.StudyDate
                    for tag in additional_tags or []:
                        setattr(ds, tag, getattr(dataset, tag))

                    patient_id_to_datasets[patient_id] = ds

        return list(patient_id_to_datasets.values())

    def studies_for_patient(self, patient_id, additional_tags=None):
        # additional tags are ignored here; only tags available are already in the files
        study_id_to_dataset = {}

        # Return one dataset per study
        for dataset in self.dicom_datasets.values():
            if patient_id == dataset.PatientID and dataset.StudyInstanceUID not in study_id_to_dataset:
                study_id_to_dataset[dataset.StudyInstanceUID] = dataset
        return study_id_to_dataset.values()

    def series_for_study(self, study_id, modality_filter=None, additional_tags=None):
        series = []
        for dataset in self.dicom_datasets.values():
            print(dataset.StudyInstanceUID)

        # Build series-level datasets from the instance-level test data
        series_id_to_dataset = {}
        for dataset in self.dicom_datasets.values():
            study_matches = dataset.StudyInstanceUID == study_id
            modality_matches = modality_filter is None or getattr(series, 'Modality', '') in modality_filter
            if study_matches and modality_matches:
                dataset.PacsmanPrivateIdentifier = 'pacsman'
                series_id = dataset.SeriesInstanceUID
                if series_id in series_id_to_dataset:
                    series_id_to_dataset[series_id].NumberOfImagesInSeries += 1
                else:
                    dataset.NumberOfImagesInSeries = 1
                    series_id_to_dataset[series_id] = dataset

        return series_id_to_dataset.values()

    def fetch_images_as_files(self, series_id):
        result_dir = os.path.join(self.dicom_dir, series_id)
        os.makedirs(result_dir, exist_ok=True)
        for (path, ds) in self.dicom_datasets.items():
            if ds.SeriesInstanceUID == series_id:
                shutil.copy(path, os.path.join(result_dir))

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
