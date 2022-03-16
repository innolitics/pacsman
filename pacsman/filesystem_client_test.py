import pytest
import os

from .filesystem_dev_client import FilesystemDicomClient


@pytest.fixture(scope="module")
def filesystem_client():
    file_dir = os.path.dirname(os.path.abspath(__file__))
    dicom_source_dir = os.path.join(file_dir, 'test_dicom_data')
    return FilesystemDicomClient(dicom_dir='.', dicom_source_dir=dicom_source_dir,
                                 client_ae="asdf")


def test_no_study_date_filter(filesystem_client):
    studies = filesystem_client.studies_for_patient('N-Lymphoma')
    assert len(studies) == 5


def test_study_date_filter_none(filesystem_client):
    date_filter = '20190101-20200101'
    studies = filesystem_client.studies_for_patient('N-Lymphoma', date_filter)
    assert len(studies) == 0


def test_study_date_filter_one(filesystem_client):
    date_filter = '20000101-20000102'
    studies = filesystem_client.studies_for_patient('N-Lymphoma', date_filter)
    assert len(studies) == 1


def test_study_date_filter_one_include_end(filesystem_client):
    date_filter = '19991231-20000101'
    studies = filesystem_client.studies_for_patient('N-Lymphoma', date_filter)
    assert len(studies) == 1


def test_study_date_filter_some(filesystem_client):
    date_filter = '20000101-20000305'
    studies = filesystem_client.studies_for_patient('N-Lymphoma', date_filter)
    assert len(studies) == 3


def test_study_date_filter_all(filesystem_client):
    date_filter = '20000101-20010101'
    studies = filesystem_client.studies_for_patient('N-Lymphoma', date_filter)
    assert len(studies) == 5
