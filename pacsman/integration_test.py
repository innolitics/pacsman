'''
These tests depend on data from www.dicomserver.co.uk, both on their remote DICOM server
and with the data retrieved to a local Horos instance. The data there could change.

No data is fetched from the remote server because C-GET is not yet supported in pacsman.

Steps to run integration tests:

 1) Configure Horos listener to run on port 11112.
 2) Add www.dicomserver.co.uk:11112 as a location in Horos with any AETitle
 3) Add localhost:11113 as a location in Horos with AETitle "TEST-SCP"
 4) Query PAT014 on the remote server on Horos and retrieve all images.
    * If not available the tests will need to be changed
 5) `pytest integration_test.py` or `pytest -m local integration_test.py` for local-only
'''

import os
import pytest
import logging
from pynetdicom_client import PynetdicomClient

dicom_clients = [PynetdicomClient]


@pytest.fixture(scope="module", params=dicom_clients)
def local_client(request):
    logger = logging.getLogger(str(request.param))
    stream_logger = logging.StreamHandler()
    logger.addHandler(stream_logger)
    logger.setLevel(logging.DEBUG)
    # local (Horos, all PAT014 data pulled from dicomserver.co.uk)
    return request.param(client_ae='TEST', pacs_url='localhost',
                         pacs_port=11112, dicom_dir='.')


@pytest.fixture(scope="module", params=dicom_clients)
def remote_client(request):
    logger = logging.getLogger(str(request.param))
    stream_logger = logging.StreamHandler()
    logger.addHandler(stream_logger)
    logger.setLevel(logging.DEBUG)
    pynetdicom_logger = logging.getLogger('pynetdicom3')
    pynetdicom_logger.setLevel(logging.DEBUG)
    return request.param(client_ae='TEST', pacs_url='www.dicomserver.co.uk',
                         pacs_port=11112, dicom_dir='.')


@pytest.mark.local
def test_verify_c_echo(local_client):
    assert local_client.verify()


@pytest.mark.local
def test_local_patient_search(local_client):
    patient_datasets = local_client.search_patients('PAT014')
    assert len(patient_datasets) == 1
    assert len(patient_datasets[0].PatientStudyIDs) > 1
    assert patient_datasets[0].PatientMostRecentStudyDate


@pytest.mark.local
def test_local_series_for_study(local_client):
    # this series is for patient PAT014
    series_datasets = local_client.series_for_study('1.2.826.0.1.3680043.11.118')
    assert len(series_datasets) > 1
    for ds in series_datasets:
        assert ds.NumberOfImagesInSeries >= 1


@pytest.mark.local
def test_local_fetch(local_client, tmpdir):
    series_id = '1.2.826.0.1.3680043.6.51581.36765.20180518132103.25992.1.21'
    local_client.dicom_dir = str(tmpdir)
    local_client.fetch_images_as_files(series_id)

    series_dir = os.path.join(tmpdir, series_id)
    assert os.path.isdir(series_dir)
    assert len(os.listdir(series_dir)) > 1


@pytest.mark.local
def test_local_fetch_thumbnail(local_client, tmpdir):
    series_id = '1.2.826.0.1.3680043.6.51581.36765.20180518132103.25992.1.21'
    local_client.dicom_dir = tmpdir
    local_client.fetch_thumbnail(series_id)
    assert len(os.listdir(tmpdir)) == 1


@pytest.mark.local
def test_local_fetch_fail(local_client, tmpdir):
    series_id = 'nonexistentseriesID'
    local_client.dicom_dir = tmpdir
    result_dir = local_client.fetch_images_as_files(series_id)
    thumbnail_file = local_client.fetch_thumbnail(series_id)
    assert result_dir is None
    assert thumbnail_file is None


@pytest.mark.remote
def test_verify_c_echo_remote(remote_client):
    assert remote_client.verify()


@pytest.mark.remote
def test_remote_patient_search(remote_client):
    patient_datasets = remote_client.search_patients('PAT014')
    assert len(patient_datasets) >= 1
    for ds in patient_datasets:
        assert ds.PatientID == 'PAT014'
        assert ds.PatientMostRecentStudyDate
        assert ds.PatientStudyIDs


@pytest.mark.remote
def test_remote_series_for_study(remote_client):
    # this series is for patient PAT014
    series_datasets = remote_client.series_for_study('1.2.826.0.1.3680043.11.119')
    assert len(series_datasets) > 1


@pytest.mark.remote
def test_remote_fetch_fail(remote_client):
    # on dicomserver.co.uk, fails with 'Unknown Move Destination: TEST-SCP'
    with pytest.raises(Exception):
        remote_client.fetch_images_as_files('1.2.826.0.1.3680043.6.79369.13951.20180518132058.25992.1.15')
