'''
Remote tests depend on data from www.dicomserver.co.uk. The data there could change.
No data is fetched from the remote server because C-GET is not yet supported in pacsman.

Local tests depend on data in the `test_dicom_dir` directory, which is imported into a local
Horos instance.

Steps to run integration tests:

 1) Configure Horos listener to run on port 11112.
 2*) In Horos: File -> Import -> Import Files and import all files in the 'test_dicom_data' dir
 3) Add localhost:11113 as a location in Horos with AETitle "TEST-SCP"
 4) `pytest integration_test.py` or `pytest -m local integration_test.py` for local-only

An alternative to step 2 above is to run upload_test.py

To explore or debug the remote data interactively, add www.dicomserver.co.uk:11112 as a
location in Horos with any AETitle.

If horos is running on a different machine set the LOCAL_PACS_URL environment variable to
the ip of the machine running horos and similarly replace localhost in step
3 of the instructions above.

'''

import logging
import os

import pytest

from .filesystem_dev_client import FilesystemDicomClient
from .pynetdicom_client import PynetDicomClient


def initialize_pynetdicom_client(client_ae, pacs_url, pacs_port, dicom_dir):
    return PynetDicomClient(client_ae=client_ae, remote_ae='TEST', pacs_url=pacs_url, pacs_port=pacs_port,
                            dicom_dir=dicom_dir)


def initialize_filesystem_client(dicom_dir, *args, **kwargs):
    file_dir = os.path.dirname(os.path.abspath(__file__))
    dicom_source_dir = os.path.join(file_dir, 'test_dicom_data')
    return FilesystemDicomClient(dicom_dir=dicom_dir, dicom_source_dir=dicom_source_dir,
                                 client_ae="asdf")


dicom_client_initializers = [initialize_pynetdicom_client, initialize_filesystem_client]

LOCAL_PACS_URL = os.environ.get('LOCAL_PACS_URL', 'localhost')


@pytest.fixture(scope="module", params=dicom_client_initializers)
def local_client(request):
    logger = logging.getLogger(str(request.param))
    stream_logger = logging.StreamHandler()
    logger.addHandler(stream_logger)
    logger.setLevel(logging.DEBUG)
    pynetdicom_logger = logging.getLogger('pynetdicom3')
    pynetdicom_logger.setLevel(logging.DEBUG)
    # local (Horos, all PAT014 data pulled from dicomserver.co.uk)
    return request.param(client_ae='TEST', pacs_url=LOCAL_PACS_URL,
                         pacs_port=11112, dicom_dir='.')


@pytest.fixture(scope="module", params=dicom_client_initializers)
def remote_client(request):
    logger = logging.getLogger(str(request.param))
    stream_logger = logging.StreamHandler()
    logger.addHandler(stream_logger)
    logger.setLevel(logging.DEBUG)
    pynetdicom_logger = logging.getLogger('pynetdicom3')
    pynetdicom_logger.setLevel(logging.DEBUG)
    return request.param(client_ae='TEST', pacs_url='www.dicomserver.co.uk',
                         pacs_port=11112, dicom_dir='.')


@pytest.mark.integration
@pytest.mark.local
def test_verify_c_echo(local_client):
    assert local_client.verify()


@pytest.mark.integration
@pytest.mark.local
def test_local_patient_search(local_client):
    patient_datasets = local_client.search_patients('PAT014',
                                                    additional_tags=['PatientSex'])
    assert len(patient_datasets) == 1
    assert len(patient_datasets[0].PatientStudyInstanceUIDs) > 1
    assert patient_datasets[0].PatientMostRecentStudyDate
    assert patient_datasets[0].PatientSex == 'F'


@pytest.mark.integration
@pytest.mark.local
def test_local_patient_search_wildcard(local_client):
    patient_datasets = local_client.search_patients('*',
                                                    additional_tags=['PatientSex'])
    assert len(patient_datasets) > 1


@pytest.mark.integration
@pytest.mark.local
def test_local_series_for_study(local_client):
    # this series is for patient PAT014
    series_datasets = local_client.series_for_study('1.2.826.0.1.3680043.11.118',
                                                    modality_filter=['CT'],
                                                    additional_tags=['InstitutionName'])
    assert len(series_datasets) > 1
    assert series_datasets[0]
    for ds in series_datasets:
        assert ds.NumberOfSeriesRelatedInstances >= 1
        assert ds.InstitutionName


@pytest.mark.integration
@pytest.mark.local
def test_local_studies_for_patient(local_client):
    studies_datasets = local_client.studies_for_patient('PAT014')

    assert len(studies_datasets) > 1
    assert studies_datasets[0]
    for ds in studies_datasets:
        assert ds.StudyInstanceUID


@pytest.mark.integration
@pytest.mark.local
def test_local_fetch(local_client, tmpdir):
    series_id = '1.2.826.0.1.3680043.6.51581.36765.20180518132103.25992.1.21'
    local_client.dicom_dir = str(tmpdir)
    local_client.fetch_images_as_dicom_files(series_id)

    series_dir = os.path.join(tmpdir, series_id)
    assert os.path.isdir(series_dir)
    assert len(os.listdir(series_dir)) > 1


@pytest.mark.integration
@pytest.mark.local
def test_local_fetch_thumbnail(local_client, tmpdir):
    # Patient ID E3148
    series_id = '1.2.392.200193.3.1626980217.161129.153348.41538611151089740341'
    local_client.dicom_dir = tmpdir
    thumbnail_path = local_client.fetch_thumbnail(series_id)
    assert thumbnail_path
    assert len(os.listdir(tmpdir)) == 1


@pytest.mark.integration
@pytest.mark.local
def test_local_fetch_fail(local_client, tmpdir):
    series_id = 'nonexistentseriesID'
    local_client.dicom_dir = tmpdir
    result_dir = local_client.fetch_images_as_dicom_files(series_id)
    thumbnail_file = local_client.fetch_thumbnail(series_id)
    assert result_dir is None
    assert thumbnail_file is None


@pytest.mark.integration
@pytest.mark.remote
def test_verify_c_echo_remote(remote_client):
    assert remote_client.verify()


@pytest.mark.integration
@pytest.mark.remote
def test_remote_patient_search(remote_client):
    patient_datasets = remote_client.search_patients('PAT014')
    assert len(patient_datasets) >= 1
    for ds in patient_datasets:
        assert ds.PatientID == 'PAT014'
        assert ds.PatientMostRecentStudyDate
        assert ds.PatientStudyInstanceUIDs


@pytest.mark.integration
@pytest.mark.remote
@pytest.mark.skip(reason="data is missing on remote server")
def test_remote_series_for_study(remote_client):
    # this series is for patient PAT014
    series_datasets = remote_client.series_for_study('1.2.826.0.1.3680043.11.118')
    assert len(series_datasets) > 1


@pytest.mark.integration
@pytest.mark.remote
def test_remote_fetch_fail(remote_client):
    # Skip failure check for dummy client (which never fails)
    if isinstance(remote_client, FilesystemDicomClient):
        return

    # on dicomserver.co.uk, fails with 'Unknown Move Destination: TEST-SCP'
    with pytest.raises(Exception):
        remote_client.fetch_images_as_dicom_files('1.2.826.0.1.3680043.6.79369.13951.20180518132058.25992.1.15')
