from abc import ABC, abstractmethod
from typing import List, Optional, Iterable

import pydicom
from pydicom import Dataset
from pydicom.valuerep import MultiValue
from pydicom.uid import UID

from .utils import getattr_required, copy_dicom_attributes


def _extend_datadict(datadict, tags):
    for tag in tags:
        try:
            existing_tag = datadict.get_entry(tag)
            if existing_tag != pacsman_private_tags[tag]:
                raise Exception(f'Private tag {tag} with different value already exists')
        except KeyError:
            pass
    datadict.add_private_dict_entries('pacsman', pacsman_private_tags)


# See this page in the DICOM standard for details on private tags:
# http://dicom.nema.org/medical/dicom/current/output/html/part05.html#sect_7.8
PRIVATE_ID = 'pacsman'

pacsman_private_tags = {
    0x00090010: ('LO', '1', 'Pacsman Private Identifier', '', 'PacsmanPrivateIdentifier'),
    0x00091001: ('UI', '1-N', 'Study Instance UIDs for Patient', '', 'PatientStudyInstanceUIDs'),
    0x00091002: ('DA', '1', 'Most Recent Study Date for Patient', '', 'PatientMostRecentStudyDate'),
}
_extend_datadict(pydicom.datadict, pacsman_private_tags)


class BaseDicomClient(ABC):
    @abstractmethod
    def verify(self) -> bool:
        """
        Send C-ECHO to PACS to verify connection
        :return: True on success, False on failure
        """
        raise NotImplementedError()

    @abstractmethod
    def search_patients(self, search_query: str, additional_tags: List[str] = None,
                        wildcard: bool = True) -> List[Dataset]:
        """
        Search for patients. The PatientID and PatientName are searched.
        Performs a partial match.
        :param search_query: Search string for either patient name or ID
        :param additional_tags: additional DICOM tags for result datasets
        :param wildcard: whether to add wildcards to search string, defaults to True
        :return: List of patient-Level pydicom Datasets, with tags:
            PatientName
            PatientID
            PatientBirthDate
            PatientStudyInstanceUIDs (private tag)
            PatientMostRecentStudyDate (private tag)
            Any valid DICOM tags in `additional_tags`
        """
        raise NotImplementedError()

    @abstractmethod
    def search_series(self, query_dataset, additional_tags=None) -> List[Dataset]:
        """
        Uses C-FIND to get patients matching the input (one req for id, one for name)
        :param query_dataset: Search dataset
        :param additional_tags: additional DICOM tags for result datasets
        :return: List of patient-Level pydicom Datasets, with tags:
            PatientName
            PatientID
            PatientBirthDate
            PatientStudyInstanceUIDs (private tag)
            PatientMostRecentStudyDate (private tag)
            Any valid DICOM tags in `additional_tags`
        """
        raise NotImplementedError()

    @abstractmethod
    def studies_for_patient(self, patient_id, study_date_tag=None, additional_tags=None) -> List[Dataset]:
        """
        Uses C-FIND to get study IDs for a patient.
        :param patient_id: Exact patient ID from PACS
        :param study_date_tag: Optional string in DICOM range format (e.g. '20200101-20200231') for studies
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
    def series_for_study(self, study_id, modality_filter=None, additional_tags=None,
                         manual_count=True) -> List[Dataset]:
        """
        :param study_id: StudyInstanceUID from PACS
        :param modality_filter: List of modalities to filter results on
        :param manual_count: if the PACS doesn't have the NumberOfSeriesRelatedInstances
          attribute, count it manually with an image level C-FIND per series.
        :param additional_tags: List of additioanl DICOM tags to add to result datasets
        :return: List of series-level pydicom Datasets, with tags:
            SeriesInstanceUID
            SeriesDescription
            SeriesDate
            SeriesTime
            Modality
            BodyPartExamined
            PatientPosition
            NumberOfSeriesRelatedInstances
            Any valid DICOM tags in `additional_tags`
        """
        raise NotImplementedError()

    def images_for_series(self, study_id, series_id, additional_tags=None, max_count=None) -> List[Dataset]:
        """
        :param study_id: StudyInstanceUID from PACS
        :param series_id: SeriesInstanceUID from PACS
        :param additional_tags:  List of additioanl DICOM tags to add to result datasets
        :param max_count: if not None then limits the number of images returned
        :return: list of image datasets
        """
        raise NotImplementedError()

    @abstractmethod
    def fetch_images_as_dicom_files(self, study_id: str, series_id: str) -> Optional[str]:
        """
        Fetches series images from PACS with C-MOVE
        :param study_id: StudyInstanceUID from PACS
        :param series_id: SeriesInstanceUID from PACS
        :return: a path to a directory full of dicom files on success, None if not found
        """
        raise NotImplementedError

    @abstractmethod
    def fetch_image_as_dicom_file(self, study_id: str, series_id: str, sop_instance_id: str) -> Optional[str]:
        """
        Fetches single series image from PACS with C-MOVE
        :param study_id: StudyInstanceUID from PACS
        :param series_id: SeriesInstanceUID from PACS
        :param sop_instance_id: SOPInstanceUID from PACS
        :return: a path to the dicom file on success, None if not found
        """
        raise NotImplementedError

    @abstractmethod
    def fetch_thumbnail(self, study_id: str, series_id: str) -> Optional[str]:
        """
        Fetches a central slice of a series from PACS and converts to PNG
        :param study_id: StudyInstanceUID from PACS
        :param series_id: SeriesInstanceUID from PACS
        :return: A path to a PNG file on success, None if not found
        """
        raise NotImplementedError

    @abstractmethod
    def fetch_slice_thumbnail(self, study_id: str, series_id: str,
                              instance_id: str) -> Optional[str]:
        """
        Fetches a specific slice of a series from PACS and converts to PNG
        :param study_id: StudyInstanceUID from PACS
        :param series_id: SeriesInstanceUID from PACS
        :param series_id: SeriesInstanceUID from PACS
        :param instance_id: SOPInstanceUID from PACS
        :return: A path to a PNG file on success, None if not found
        """
        raise NotImplementedError

    @staticmethod
    def update_patient_result(result, dataset, additional_tags=None):
        patient_id = getattr_required(dataset, 'PatientID')
        study_instance_uid = getattr_required(dataset, 'StudyInstanceUID')

        # Most of the data for a particular patient search result is grabbed
        # the first time this method is called for a patient.  This behaviour
        # may change in the future, e.g., we use the most recent attribute
        # values and/or combine attribute values from multiple different
        # datasets, when that data is missing in one or the other.
        # For now, we assume that the first time this method is called, the
        # `result` is an empty dataset.
        if len(result) == 0:
            result.PatientID = patient_id
            result.PatientName = getattr(dataset, 'PatientName', '')
            result.PatientBirthDate = getattr(dataset, 'PatientBirthDate', '')
            result.PatientStudyInstanceUIDs = MultiValue(UID, [study_instance_uid])
            result.PacsmanPrivateIdentifier = PRIVATE_ID
            result.PatientMostRecentStudyDate = getattr(dataset, 'StudyDate', '')
            copy_dicom_attributes(result, dataset, additional_tags, missing='empty')
        else:
            if result.PatientID != patient_id:
                raise ValueError("The search result has a different patient ID")

            existing_uids = {uid.name for uid in result.PatientStudyInstanceUIDs}
            if study_instance_uid.name not in existing_uids:
                result.PatientStudyInstanceUIDs.append(study_instance_uid)

        study_date = getattr(dataset, 'StudyDate', '')
        if study_date != '':
            no_existing_date = result.PatientMostRecentStudyDate == ''
            if no_existing_date or study_date > result.PatientMostRecentStudyDate:
                result.PatientMostRecentStudyDate = study_date

    @abstractmethod
    def send_datasets(self, datasets: Iterable[Dataset], override_remote_ae: str = None,
                      override_pacs_url: str = None, override_pacs_port: int = None) -> None:
        """
        Send a dicom dataset
        :param datasets: datasets to send
        :param override_remote_ae: override this clients stored AE and send to different remote
        :param override_pacs_url: conditionally required with override_remote_ae
        :param override_pacs_port: conditionally required with override_remote_ae
        :return:
        """
        raise NotImplementedError
