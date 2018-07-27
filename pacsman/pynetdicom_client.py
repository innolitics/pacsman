from contextlib import contextmanager
from itertools import chain
import logging
import os
import threading

from pydicom import dcmread
from pydicom.dataset import Dataset, FileDataset
from pydicom.uid import ExplicitVRLittleEndian
from pydicom.valuerep import MultiValue
from pynetdicom3 import AE, QueryRetrieveSOPClassList, StorageSOPClassList, \
    pynetdicom_version, pynetdicom_implementation_uid
from pynetdicom3.pdu_primitives import SCP_SCU_RoleSelectionNegotiation


from .dicom_interface import DicomInterface
from .utils import process_and_write_png

logger = logging.getLogger(__name__)

# http://dicom.nema.org/medical/dicom/current/output/html/part07.html#chapter_C
status_success_or_pending = [0x0000, 0xFF00, 0xFF01]


class PynetdicomClient(DicomInterface):

    def verify(self):

        ae = AE(ae_title=self.client_ae, scu_sop_class=['1.2.840.10008.1.1'])
        # setting timeout here doesn't appear to have any effect
        ae.network_timeout = self.timeout

        with association(ae, self.pacs_url, self.pacs_port) as assoc:
            logger.debug('Association accepted by the peer')
            # Send a DIMSE C-ECHO request to the peer
            # status is a pydicom Dataset object with (at a minimum) a
            # (0000, 0900) Status element
            status = assoc.send_c_echo()

            # Output the response from the peer
            if status.Status in status_success_or_pending:
                logger.debug('C-ECHO Response: 0x{0:04x}'.format(status.Status))
                return True
            else:
                logger.warning('C-ECHO Failure Response: 0x{0:04x}'.format(status.Status))
                return False

        return False

    def search_patients(self, search_query, additional_tags=None):

        ae = AE(ae_title=self.client_ae, scu_sop_class=QueryRetrieveSOPClassList)

        with association(ae, self.pacs_url, self.pacs_port) as assoc:
            # perform first search on patient ID
            id_responses = _call_c_find_patients(assoc, 'PatientID',
                                                 f'*{search_query}*', additional_tags)
            # perform second search on patient name
            name_responses = _call_c_find_patients(assoc, 'PatientName',
                                                   f'*{search_query}*', additional_tags)

            uid_to_result = {}
            for dataset in chain(checked_responses(id_responses),
                                 checked_responses(name_responses)):
                if hasattr(dataset, 'PatientID'):
                    # remove non-unique Study UIDs
                    #  (some dupes are returned, especially for ID search)
                    uid_to_result[dataset.StudyInstanceUID] = dataset

            # separate by patient ID, count studies and get most recent
            patient_id_to_datasets = {}
            for study in uid_to_result.values():
                patient_id = study.PatientID

                if patient_id in patient_id_to_datasets:
                    if study.StudyDate > patient_id_to_datasets[patient_id].PatientMostRecentStudyDate:
                        patient_id_to_datasets[patient_id].PatientMostRecentStudyDate = study.StudyDate

                    patient_id_to_datasets[patient_id].PatientStudyIDs.append(study.StudyInstanceUID)
                else:
                    ds = Dataset()
                    ds.PatientID = patient_id
                    ds.PatientName = study.PatientName
                    ds.PatientBirthDate = study.PatientBirthDate
                    ds.PatientStudyIDs = MultiValue(str, study.StudyInstanceUID)

                    ds.PacsmanPrivateIdentifier = 'pacsman'
                    ds.PatientMostRecentStudyDate = study.StudyDate
                    for tag in additional_tags or []:
                        setattr(ds, tag, getattr(study, tag))

                    patient_id_to_datasets[patient_id] = ds

            return list(patient_id_to_datasets.values())

    def studies_for_patient(self, patient_id, additional_tags=None):
        ae = AE(ae_title=self.client_ae, scu_sop_class=QueryRetrieveSOPClassList)

        with association(ae, self.pacs_url, self.pacs_port) as assoc:
            responses = _call_c_find_patients(assoc, 'PatientID', f'{patient_id}', additional_tags)

            datasets = []
            for dataset in checked_responses(responses):
                # Some PACS send back empty "Success" responses at the end of the list
                if hasattr(dataset, 'PatientID'):
                    datasets.append(dataset)

            return datasets

    def series_for_study(self, study_id, modality_filter=None, additional_tags=None):

        ae = AE(ae_title=self.client_ae, scu_sop_class=QueryRetrieveSOPClassList)

        with association(ae, self.pacs_url, self.pacs_port) as assoc:
            dataset = Dataset()
            dataset.StudyInstanceUID = study_id

            # Filtering modality with 'MR\\CT' doesn't seem to work with pynetdicom
            dataset.Modality = ''
            dataset.BodyPartExamined = ''
            dataset.SeriesDescription = ''
            dataset.SeriesDate = ''
            dataset.SeriesTime = ''
            dataset.SeriesInstanceUID = ''
            dataset.PatientPosition = ''
            dataset.QueryRetrieveLevel = 'SERIES'

            for tag in additional_tags or []:
                setattr(dataset, tag, '')

            responses = assoc.send_c_find(dataset, query_model='S')

            series_datasets = []
            for series in checked_responses(responses):
                if hasattr(series, 'SeriesInstanceUID') and (modality_filter is None or
                   getattr(series, 'Modality', '') in modality_filter):
                    ds = Dataset()
                    ds.SeriesDescription = getattr(series, 'SeriesDescription', '')
                    ds.BodyPartExamined = getattr(series, 'BodyPartExamined', None)
                    ds.SeriesInstanceUID = series.SeriesInstanceUID
                    ds.Modality = series.Modality
                    ds.SeriesDate = series.SeriesDate
                    ds.SeriesTime = series.SeriesTime
                    for tag in additional_tags or []:
                        setattr(ds, tag, getattr(series, tag))

                    with association(ae, self.pacs_url, self.pacs_port) as series_assoc:
                        series_dataset = Dataset()
                        series_dataset.SeriesInstanceUID = series.SeriesInstanceUID
                        series_dataset.QueryRetrieveLevel = 'IMAGE'
                        series_dataset.SOPInstanceUID = ''

                        series_responses = series_assoc.send_c_find(series_dataset, query_model='S')
                        image_ids = []
                        for instance in checked_responses(series_responses):
                            if hasattr(instance, 'SOPInstanceUID'):
                                image_ids.append(instance.SOPInstanceUID)

                    ds.PacsmanPrivateIdentifier = 'pacsman'
                    ds.NumberOfImagesInSeries = len(image_ids)

                    series_datasets.append(ds)

        return series_datasets

    def fetch_images_as_files(self, series_id):

        series_path = os.path.join(self.dicom_dir, series_id)

        with storage_scp(self.client_ae, series_path) as scp:
            ae = AE(ae_title=self.client_ae,
                    scu_sop_class=QueryRetrieveSOPClassList,
                    transfer_syntax=[ExplicitVRLittleEndian])

            extended_negotiation_info = []
            for context in ae.presentation_contexts_scu:
                negotiation = SCP_SCU_RoleSelectionNegotiation()
                negotiation.sop_class_uid = context.abstract_syntax
                negotiation.scu_role = False
                negotiation.scp_role = True
                extended_negotiation_info.append(negotiation)

            with association(ae, self.pacs_url, self.pacs_port,
                             ext_neg=extended_negotiation_info) as assoc:
                dataset = Dataset()
                dataset.SeriesInstanceUID = series_id
                dataset.QueryRetrieveLevel = 'IMAGE'

                if scp.is_alive():
                    responses = assoc.send_c_move(dataset, scp.ae_title,
                                                  query_model='S')
                else:
                    raise Exception(f'Storage SCP failed to start for series {series_id}')

                for _ in checked_responses(responses):
                    # just check response Status
                    pass

                return series_path if os.path.exists(series_path) else None

    def fetch_thumbnail(self, series_id):
        ae = AE(ae_title=self.client_ae, scu_sop_class=QueryRetrieveSOPClassList)

        with association(ae, self.pacs_url, self.pacs_port) as assoc:
            # search for image IDs in the series
            find_dataset = Dataset()
            find_dataset.SeriesInstanceUID = series_id
            find_dataset.QueryRetrieveLevel = 'IMAGE'
            find_dataset.SOPInstanceUID = ''
            find_response = assoc.send_c_find(find_dataset, query_model='S')

            image_ids = []
            for dataset in checked_responses(find_response):
                if hasattr(dataset, 'SOPInstanceUID'):
                    image_ids.append(dataset.SOPInstanceUID)

            if not image_ids:
                return None

            with storage_scp(self.client_ae, self.dicom_dir) as scp:
                # get the middle image in the series for the thumbnail
                middle_image_id = image_ids[len(image_ids) // 2]
                move_dataset = Dataset()
                move_dataset.SOPInstanceUID = middle_image_id
                move_dataset.QueryRetrieveLevel = 'IMAGE'

                if scp.is_alive():
                    move_responses = assoc.send_c_move(move_dataset, scp.ae_title,
                                                       query_model='S')
                else:
                    raise Exception(f'Storage SCP failed to start for series {series_id}')

                for _ in checked_responses(move_responses):
                    # just check response Status
                    pass

                dcm_path = os.path.join(self.dicom_dir, f'{middle_image_id}.dcm')
                if not os.path.exists(dcm_path):
                    return None

                try:
                    thumbnail_ds = dcmread(dcm_path)
                    png_path = os.path.splitext(dcm_path)[0] + '.png'
                    process_and_write_png(thumbnail_ds, png_path)
                    return png_path
                except Exception as e:
                    logger.error(f'Thumbnail PNG conversion failed: {e}')
                    return None
                finally:
                    os.remove(dcm_path)


def _call_c_find_patients(assoc, search_field, search_query, additional_tags=None):
    dataset = Dataset()

    dataset.PatientID = None
    dataset.PatientName = ''
    dataset.PatientBirthDate = None
    dataset.StudyDate = ''
    dataset.StudyInstanceUID = ''
    dataset.QueryRetrieveLevel = 'STUDY'

    setattr(dataset, search_field, search_query)

    for tag in additional_tags or []:
        setattr(dataset, tag, '')

    return assoc.send_c_find(dataset, query_model='S')


class StorageSCP(threading.Thread):
    def __init__(self, client_ae, result_dir):
        self.result_dir = result_dir

        self.ae_title = f'{client_ae}-SCP'
        self.ae = AE(ae_title=self.ae_title,
                     port=11113,
                     transfer_syntax=[ExplicitVRLittleEndian],
                     scp_sop_class=[x for x in StorageSOPClassList])

        self.ae.on_c_store = self._on_c_store

        threading.Thread.__init__(self)

        self.daemon = True

    def run(self):
        """The thread run method"""
        self.ae.start()

    def stop(self):
        """Stop the SCP thread"""
        self.ae.stop()

    def _on_c_store(self, dataset, context, info):
        '''
        :param dataset: pydicom.Dataset
            The DICOM dataset sent via the C-STORE
        :param context: pynetdicom3.presentation.PresentationContextTuple
            Details of the presentation context the dataset was sent under.
        :param info: dict
            A dict containing information about the association and DIMSE message.
        :return: pynetdicom.sop_class.Status or int
        '''
        try:

            os.makedirs(self.result_dir, exist_ok=True)

            filename = f'{dataset.SOPInstanceUID}.dcm'
            filepath = os.path.join(self.result_dir, filename)

            logger.info(f'Storing DICOM file: {filepath}')

            if os.path.exists(filename):
                logger.warning('DICOM file already exists, overwriting')

            meta = Dataset()
            meta.MediaStorageSOPClassUID = dataset.SOPClassUID
            meta.MediaStorageSOPInstanceUID = dataset.SOPInstanceUID
            meta.ImplementationClassUID = pynetdicom_implementation_uid
            meta.TransferSyntaxUID = context.transfer_syntax

            # The following is not mandatory, set for convenience
            meta.ImplementationVersionName = pynetdicom_version

            ds = FileDataset(filepath, {}, file_meta=meta, preamble=b"\0" * 128)
            ds.update(dataset)
            ds.is_little_endian = context.transfer_syntax.is_little_endian

            ds.is_implicit_VR = context.transfer_syntax.is_implicit_VR
            ds.save_as(filepath, write_like_original=False)

            status_ds = Dataset()
            status_ds.Status = 0x0000
        except Exception as e:
            logger.error(f'C-STORE failed: {e}')
            status_ds = Dataset()
            status_ds.Status = 0x0110  # Processing Failure
        return status_ds


@contextmanager
def association(ae, pacs_url, pacs_port, *args, **kwargs):
    try:
        assoc = ae.associate(pacs_url, pacs_port, *args, **kwargs)
        if assoc.is_established:
            yield assoc
        elif assoc.is_rejected:
            raise ConnectionError(f'Association rejected with {pacs_url}')
        elif assoc.is_aborted:
            raise ConnectionError(f'Received A-ABORT during association with {pacs_url}')
        else:
            raise ConnectionError(f'Failed to establish association with {pacs_url}')
    except Exception as e:
        raise e
    finally:
        assoc.release()


@contextmanager
def storage_scp(client_ae, result_dir):
    try:
        scp = StorageSCP(client_ae, result_dir)
        scp.start()
        yield scp
    except Exception as e:
        raise e
    finally:
        scp.stop()


def checked_responses(responses):
    '''
    Generator for checking success or pending status of DICOM responses
    Success response may only come once at the end of the dataset response list.

    :param responses: List of (Status, Dataset) tuples from pynetdicom call
    :return: List of Datasets or exception on warning/abort/failure
    '''
    for (status, dataset) in responses:
        logger.debug(status)
        logger.debug(dataset)
        if status.Status in status_success_or_pending:
            yield dataset
        else:
            raise Exception('DICOM Response Failed With Status: 0x{0:04x}'.format(status.Status))
