
from abc import ABC, abstractmethod
from collections import namedtuple


PatientInfo = namedtuple('PatientInfo', ['first_name', 'last_name', 'patient_id', 'dob',
                                          'study_ids', 'most_recent_study'])

SeriesInfo = namedtuple('SeriesInfo', ['series_id', 'acquisition_datetime', 'description', 'modality',
                                       'num_images'])


class DicomInterface(ABC):

    def __init__(self, client_ae, pacs_url, pacs_port, dicom_dir, timeout=5):
        self.client_ae = client_ae
        self.pacs_url = pacs_url
        self.pacs_port = pacs_port
        self.dicom_dir = dicom_dir
        # connection timeout in s
        self.timeout = timeout

    @abstractmethod
    def verify(self):
        """
        Send C-ECHO to PACS to verify connection
        :return: True on success, False on failure
        """
        raise NotImplementedError()

    @abstractmethod
    def search_patients(self, search_query):
        """
        Uses C-FIND to get patients matching the input (one req for id, one for name)
        :param patient_input: Search string for either patient name or ID
        :return: List of PatientInfo
        """
        raise NotImplementedError()

    @abstractmethod
    def studies_for_patient(self, patient_id):
        """
        Uses C-FIND to get study IDs for a patient.
        :param patient_id: Exact patient ID from PACS
        :return: List of study IDs as strings
        """
        raise NotImplementedError()

    @abstractmethod
    def series_for_study(self, study_id, modality_filter=None):
        """
        :param study_id: StudyInstanceUID from PACS
        :param modality_filter: List of modalities to filter results on
        :return: List of SeriesInfo
        """
        raise NotImplementedError()

    @abstractmethod
    def fetch_images_as_files(self, series_id):
        """
        Fetches series images from PACS with C-GET
        :param series_id: SeriesInstanceUID from PACS
        :return: a path to a directory full of dicom files on success, None if not found
        """
        raise NotImplementedError

    @abstractmethod
    def fetch_thumbnail(self, series_id):
        """
        Fetches a central slice of a series from PACS
        :param series_id: SeriesInstanceUID from PACS
        :return: A path to a dicom file on success, None if not found
        """
        raise NotImplementedError
