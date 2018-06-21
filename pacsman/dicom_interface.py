
from abc import ABC, abstractmethod
from collections import namedtuple


PatientInfo = namedtuple('PatientInfo', ['name', 'patient_id', 'dob', 'num_studies',
                                         'most_recent_study'])

SeriesInfo = namedtuple('SeriesInfo', ['acquisition_datetime', 'procedure', 'modality',
                                       'num_images'])


class DicomInterface(ABC):

    def __init__(self, client_ae, pacs_url, pacs_port, timeout=5000):
        self.client_ae = client_ae
        self.pacs_url = pacs_url
        self.pacs_port = pacs_port
        # connection timeout in ms
        self.timeout = timeout

    @abstractmethod
    def verify(self):
        """
        Send C-ECHO to PACS to verify connection
        :return: True on success, False on failure
        """
        raise NotImplementedError()

    @abstractmethod
    def search_patients(self, patient_id, since_date):
        """
        Uses C-FIND to get patients matching the input (one req for id, one for name)
        :param patient_name_search: Search string for either patient name or ID
        :return: List of PatientInfo
        """
        raise NotImplementedError()

    @abstractmethod
    def series_for_patient(self, patient_id):
        """
        :param patient_id: PatientID from PACS
        :return: List of SeriesInfo
        """
        raise NotImplementedError()

    @abstractmethod
    def fetch_images_as_files(self, series_id):
        """
        Fetches series images from PACS with C-GET
        :param series_id: SeriesInstanceUID from PACS
        :return: a path to a directory full of dicom files
        """
        raise NotImplementedError

    @abstractmethod
    def fetch_thumbnail(self, series_id):
        """
        Fetches a central slice of a series from PACS with C-GET
        :param series_id: SeriesInstanceUID from PACS
        :return: A path to a dicom file
        """
        raise NotImplementedError
