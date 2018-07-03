import logging
import os
import threading
from itertools import chain

from dicom_interface import DicomInterface, PatientInfo, SeriesInfo
from pydicom.dataset import Dataset, FileDataset
from pydicom.uid import ImplicitVRLittleEndian, ExplicitVRLittleEndian
from pynetdicom3 import AE, QueryRetrieveSOPClassList, StorageSOPClassList, \
    pynetdicom_version, pynetdicom_implementation_uid
from pynetdicom3.pdu_primitives import SCP_SCU_RoleSelectionNegotiation

logger = logging.getLogger(__name__)
stream_logger = logging.StreamHandler()
logger.addHandler(stream_logger)
logger.setLevel(logging.DEBUG)


class PynetdicomClient(DicomInterface):

    def verify(self):
        """
        :return: True on success, False on failure
        """
        ae = AE(ae_title=self.client_ae, scu_sop_class=['1.2.840.10008.1.1'])
        # setting timeout here doesn't appear to have any effect
        ae.network_timeout = self.timeout

        assoc = ae.associate(self.pacs_url, self.pacs_port)

        if assoc.is_established:
            logger.debug('Association accepted by the peer')
            # Send a DIMSE C-ECHO request to the peer
            # status is a pydicom Dataset object with (at a minimum) a
            # (0000, 0900) Status element
            status = assoc.send_c_echo()

            # Release the association
            assoc.release()

            # Output the response from the peer
            if status:
                logger.debug('C-ECHO Response: 0x{0:04x}'.format(status.Status))
                return True
        elif assoc.is_rejected:
            logger.warning('Association was rejected by the peer')
        elif assoc.is_aborted:
            logger.warning('Received an A-ABORT from the peer during Association')

        return False

    def search_patients(self, search_query):
        """
        Uses C-FIND to get patients matching the input (one req for id, one for name)
        :param patient_name_search: Search string for either patient name or ID
        :return: List of PatientInfo
        """
        ae = AE(ae_title=self.client_ae, scu_sop_class=QueryRetrieveSOPClassList)

        assoc = ae.associate(self.pacs_url, self.pacs_port)

        if assoc.is_established:

            # perform first search on patient ID
            id_responses = _call_c_find_patients(assoc, 'PatientID',
                                                 f'*{search_query}*')
            # perform second search on patient name
            name_responses = _call_c_find_patients(assoc, 'PatientName',
                                                   f'*{search_query}*')

            uid_to_result = {}
            for (status, result) in chain(id_responses, name_responses):
                logger.debug(status)
                logger.debug(result)
                if not status:
                    # TODO status codes need to be checked for Failure/Cancel:
                    # http://dicom.nema.org/MEDICAL/Dicom/2015c/output/chtml/part07/chapter_C.html
                    raise ConnectionError('PACS connection did not return valid status')
                if result:
                    # remove non-unique Study UIDs
                    #  (some dupes are returned, especially for ID search)
                    uid_to_result[result.StudyInstanceUID] = result

            # separate by patient ID, count studies and get most recent
            patient_id_to_info = {}
            for study in uid_to_result.values():
                patient_id = study.PatientID
                study_id = study.StudyInstanceUID
                if id in patient_id_to_info:
                    if study.StudyDate > patient_id_to_info[patient_id].most_recent_study:
                        most_recent_study = study.StudyDate
                    else:
                        most_recent_study = patient_id_to_info[patient_id].most_recent_study

                    prev_study_ids = patient_id_to_info[patient_id].num_studies

                    info = PatientInfo(first_name=study.PatientName.given_name,
                                       last_name=study.PatientName.family_name,
                                       dob=study.PatientBirthDate,
                                       patient_id=patient_id,
                                       most_recent_study=most_recent_study,
                                       study_ids=prev_study_ids + [study_id])
                else:
                    info = PatientInfo(first_name=study.PatientName.given_name,
                                       last_name=study.PatientName.family_name,
                                       dob=study.PatientBirthDate,
                                       patient_id=patient_id,
                                       most_recent_study=study.StudyDate,
                                       study_ids=[study_id])
                patient_id_to_info[patient_id] = info

            # Release the association
            assoc.release()

            return list(patient_id_to_info.values())
        else:
            raise ConnectionError(f'Failed to establish association with {self.pacs_url}')

    def studies_for_patient(self, patient_id):
        ae = AE(ae_title=self.client_ae, scu_sop_class=QueryRetrieveSOPClassList)

        assoc = ae.associate(self.pacs_url, self.pacs_port)
        if assoc.is_established:
            responses = _call_c_find_patients(assoc, 'PatientID', patient_id)

            study_ids = []
            for (status, result) in responses:
                study_ids.append(result.StudyInstanceUID)

            # Release the association
            assoc.release()

            return study_ids
        else:
            raise ConnectionError(f'Failed to establish association with {self.pacs_url}')

    def series_for_study(self, study_id, modality_filter=None):
        """
        :param study_id: StudyInstanceUID from PACS
        :return: SeriesInfo
        """
        ae = AE(ae_title=self.client_ae, scu_sop_class=QueryRetrieveSOPClassList)
        assoc = ae.associate(self.pacs_url, self.pacs_port)

        if assoc.is_established:
            dataset = Dataset()
            dataset.StudyInstanceUID = study_id

            # Filtering modality with 'MR\\CT' doesn't seem to work with pynetdicom
            dataset.Modality = ''
            dataset.PatientName = ''
            dataset.BodyPartExamined = ''
            dataset.SeriesDescription = ''
            dataset.SeriesDate = ''
            dataset.SeriesTime = ''
            dataset.SeriesInstanceUID = ''
            dataset.PatientPosition = ''
            dataset.QueryRetrieveLevel = 'SERIES'

            responses = assoc.send_c_find(dataset, query_model='S')

            series_infos = []
            for (status, series) in responses:
                logger.debug(status)
                logger.debug(series)

                if series and (modality_filter is None or
                               getattr(series, 'Modality', '') in modality_filter):
                    description = getattr(series, 'SeriesDescription', '')
                    body_part_examined = getattr(series, 'BodyPartExamined', None)
                    if body_part_examined:
                        description += f' ({body_part_examined})'

                    series_assoc = ae.associate(self.pacs_url, self.pacs_port)
                    series_dataset = Dataset()
                    series_dataset.SeriesInstanceUID = series.SeriesInstanceUID
                    series_dataset.QueryRetrieveLevel = 'IMAGE'
                    series_dataset.SOPInstanceUID = ''

                    series_responses = series_assoc.send_c_find(series_dataset, query_model='S')
                    image_ids = []
                    for (instance_status, instance) in series_responses:
                        logger.debug(instance)
                        if instance_status:
                            if hasattr(instance, 'SOPInstanceUID'):
                                image_ids.append(instance.SOPInstanceUID)

                    series_assoc.release()

                    info = SeriesInfo(series_id=series.SeriesInstanceUID, description=description,
                                      modality=series.Modality, num_images=len(image_ids),
                                      acquisition_datetime=series.SeriesDate)

                    series_infos.append(info)

            assoc.release()
        else:
            raise ConnectionError(f'Failed to establish association with {self.pacs_url}')

        return series_infos

    def fetch_images_as_files(self, series_id):
        """
        Fetches series images from PACS with C-MOVE/C-STORE
        :param series_id: SeriesInstanceUID from PACS
        :return: a path to a directory full of dicom files
        """
        series_path = os.path.join(self.dicom_dir, series_id)
        os.makedirs(series_path, exist_ok=True)
        scp = StorageSCP(self.client_ae, series_path)
        scp.start()

        try:
            ae = AE(ae_title=self.client_ae,
                    scu_sop_class=QueryRetrieveSOPClassList,
                    transfer_syntax=[ExplicitVRLittleEndian])

            ext_neg = []
            for context in ae.presentation_contexts_scu:
                tmp = SCP_SCU_RoleSelectionNegotiation()
                tmp.sop_class_uid = context.abstract_syntax
                tmp.scu_role = False
                tmp.scp_role = True
                ext_neg.append(tmp)

            assoc = ae.associate(self.pacs_url, self.pacs_port,
                                 ext_neg=ext_neg)
            if assoc.is_established:
                dataset = Dataset()
                dataset.SeriesInstanceUID = series_id
                dataset.QueryRetrieveLevel = 'IMAGE'

                response = assoc.send_c_move(dataset, f'{self.client_ae}-SCP',
                                             query_model='S')

                for (status, d) in response:
                    logger.debug(status)
                    logger.debug(d)
                # TODO need context manager for this
                assoc.release()

                return series_path
            else:
                raise ConnectionError(
                    f'Failed to establish association with {self.pacs_url}')

        except Exception as e:
            raise e
        finally:
            scp.stop()

    def fetch_thumbnail(self, series_id):
        """
        Fetches central slice of a series from PACS with C-GET
        :param series_id: SeriesInstanceUID from PACS
        :return: A path to a dicom file
        """
        ae = AE(ae_title=self.client_ae, scu_sop_class=QueryRetrieveSOPClassList)

        assoc = ae.associate(self.pacs_url, self.pacs_port)

        if assoc.is_established:
            # search for image IDs in the series
            find_dataset = Dataset()
            find_dataset.SeriesInstanceUID = series_id
            find_dataset.QueryRetrieveLevel = 'IMAGE'
            find_dataset.SOPInstanceUID = ''
            find_response = assoc.send_c_find(find_dataset, query_model='S')

            image_ids = []
            for (status, result) in find_response:
                logger.debug(status)
                logger.debug(result)
                if status:
                    if hasattr(result, 'SOPInstanceUID'):
                        image_ids.append(result.SOPInstanceUID)

            scp = StorageSCP(self.client_ae, self.dicom_dir)
            scp.start()
            try:
                # get the middle image in the series for the thumbnail
                middle_image_id = image_ids[len(image_ids) // 2]
                move_dataset = Dataset()
                move_dataset.SOPInstanceUID = middle_image_id
                move_dataset.QueryRetrieveLevel = 'IMAGE'

                response = assoc.send_c_move(move_dataset, f'{self.client_ae}-SCP',
                                             query_model='S')
                for (status, d) in response:
                    logger.debug(status)
                    logger.debug(d)

                return os.path.join(self.dicom_dir, f'{middle_image_id}.dcm')
            except Exception as e:
                raise e
            finally:
                assoc.release()
                scp.stop()
        else:
            raise ConnectionError(f'Failed to establish association with {self.pacs_url}')


def _call_c_find_patients(assoc, search_field, search_query):
    dataset = Dataset()

    dataset.PatientID = None
    dataset.PatientName = ''
    dataset.PatientBirthDate = None
    dataset.StudyDate = ''
    dataset.StudyInstanceUID = ''
    dataset.QueryRetrieveLevel = 'STUDY'

    setattr(dataset, search_field, search_query)

    return assoc.send_c_find(dataset, query_model='S')


class StorageSCP(threading.Thread):
    def __init__(self, client_ae, result_dir):
        self.result_dir = result_dir

        self.ae = AE(ae_title=f'{client_ae}-SCP',
                     port=40001,
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
            return status_ds
        except Exception as e:
            logger.error(f'C-STORE failed: {e}')
            status_ds = Dataset()
            status_ds.Status = 0x0110  # Processing Failure
            return status_ds


if __name__ == '__main__':

    logger.setLevel(logging.DEBUG)
    pynetdicom_logger = logging.getLogger('pynetdicom3')
    pynetdicom_logger.setLevel(logging.DEBUG)

    remote_client = PynetdicomClient(client_ae='TEST', pacs_url='www.dicomserver.co.uk',
                                     pacs_port=11112, dicom_dir='.')
    local_client = PynetdicomClient(client_ae='TEST', pacs_url='localhost',
                                    pacs_port=40000, dicom_dir='.')

    assert remote_client.verify()
    patients = remote_client.search_patients('PAT014')
    series = remote_client.series_for_study('1.2.826.0.1.3680043.11.119')

    # on dicomserver.co.uk, fails with 'Unknown Move Destination: TEST-SCP'
    remote_client.fetch_images_as_files('1.2.826.0.1.3680043.6.79369.13951.20180518132058.25992.1.15')

    # local (Horos, pulled from dicomserver.co.uk)
    assert local_client.verify()
    local_client.fetch_images_as_files('1.2.826.0.1.3680043.6.51581.36765.20180518132103.25992.1.21')
    local_client.fetch_thumbnail('1.2.826.0.1.3680043.6.51581.36765.20180518132103.25992.1.21')
