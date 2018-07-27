
from abc import ABC, abstractmethod

from pydicom import datadict

# http://dicom.nema.org/medical/dicom/current/output/html/part05.html#sect_7.8
pacsman_private_tags = {
    0x00090010: ('LO', '1', 'Pacsman Private Identifier', '', 'PacsmanPrivateIdentifier'),
    0x00091001: ('CS', '1-N', "Study IDs for Patient", '', 'PatientStudyIDs'),
    0x00091002: ('DA', '1', 'Most Recent Study Date', '', 'PatientMostRecentStudyDate'),
    0x00091003: ('UL', '1', "Number of Images in Series", '', 'NumberOfImagesInSeries'),
}


class DicomInterface(ABC):

    def __init__(self, client_ae, pacs_url, pacs_port, dicom_dir, timeout=5):
        """
        :param client_ae: Name for this client Association Entity. {client_ae}-SCP:11113
            needs to be registered with the remote PACS in order for C-MOVE to work
        :param pacs_url: Remote PACS URL
        :param pacs_port: Remote PACS port (usually 11112)
        :param dicom_dir: Root dir for storage of *.dcm files.
        :param timeout: Connection and DICOM timeout in seconds
        """
        self.client_ae = client_ae
        self.pacs_url = pacs_url
        self.pacs_port = pacs_port
        self.dicom_dir = dicom_dir
        self.timeout = timeout

        for tag in pacsman_private_tags:
            try:
                existing_tag = datadict.get_entry(tag)
                if existing_tag != pacsman_private_tags[tag]:
                    raise Exception(f'Private tag {tag} with different value already'
                                    f' exists in dictionary.')
            except KeyError:
                pass

        datadict.add_dict_entries(pacsman_private_tags)

    @abstractmethod
    def verify(self):
        """
        Send C-ECHO to PACS to verify connection
        :return: True on success, False on failure
        """
        raise NotImplementedError()

    @abstractmethod
    def search_patients(self, search_query, additional_tags=None):
        """
        Uses C-FIND to get patients matching the input (one req for id, one for name)
        :param patient_input: Search string for either patient name or ID
        :param additional_tags: additional DICOM tags for result datasets
        :return: List of patient-Level pydicom Datasets, with tags:
            PatientName
            PatientID
            PatientBirthDate
            PatientStudyIDs (private tag)
            PatientMostRecentStudyDate (private tag)
            Any valid DICOM tags in `additional_tags`
        """
        raise NotImplementedError()

    @abstractmethod
    def studies_for_patient(self, patient_id, additional_tags=None):
        """
        Uses C-FIND to get study IDs for a patient.
        :param patient_id: Exact patient ID from PACS
        :param additional_tags: additional DICOM tags for result datasets
        :return: List of pydicom Datasets with tags:
            PatientID
            StudyInstanceUID
            PatientName
            StudyDate
            Any valid DICOM tags in `additional_tags`
        """
        raise NotImplementedError()

    @abstractmethod
    def series_for_study(self, study_id, modality_filter=None, additional_tags=None):
        """
        :param study_id: StudyInstanceUID from PACS
        :param modality_filter: List of modalities to filter results on
        :param additional_tags: List of additioanl DICOM tags to add to result datasets
        :return: List of series-level pydicom Datasets, with tags:
            SeriesInstanceUID
            SeriesDescription
            SeriesDate
            SeriesTime
            Modality
            BodyPartExamined
            PatientPosition
            NumberOfImagesInSeries (private tag)
            Any valid DICOM tags in `additional_tags`
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
        Fetches a central slice of a series from PACS and converts to PNG
        :param series_id: SeriesInstanceUID from PACS
        :return: A path to a PNG file on success, None if not found or failure
        """
        raise NotImplementedError
