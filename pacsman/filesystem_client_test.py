import pytest

from pydicom import dcmwrite, Dataset
import os

from .filesystem_dev_client import FilesystemDicomClient, INDEX_FILENAME

SRC_DIR = 'test_dicom_data'


def get_dicom_source_dir():
    file_dir = os.path.dirname(os.path.abspath(__file__))
    dicom_source_dir = os.path.join(file_dir, SRC_DIR)
    return dicom_source_dir


def get_client():
    file_dir = os.path.dirname(os.path.abspath(__file__))
    dicom_source_dir = os.path.join(file_dir, SRC_DIR)
    return FilesystemDicomClient(dicom_dir='.', dicom_source_dir=dicom_source_dir,
                          client_ae="test-ae")


def get_new_dataset():
    sop_instance_id = '300'
    new_ds = Dataset()
    new_ds.PatientID = 'new01'
    new_ds.PatientName = 'New'
    new_ds.SeriesInstanceUID = '100'
    new_ds.StudyInstanceUID = '200'
    new_ds.SOPInstanceUID = sop_instance_id
    new_ds.file_meta = Dataset()
    # SOP Class: CT Image Storage
    new_ds.file_meta.MediaStorageSOPClassUID = '1.2.840.10008.5.1.4.1.1.2'
    new_ds.file_meta.MediaStorageSOPInstanceUID = sop_instance_id
    return new_ds


def remove_index():
    index_path = os.path.join(get_dicom_source_dir(), INDEX_FILENAME)
    if os.path.exists(index_path):
        os.remove(index_path)
    assert not os.path.exists(index_path)


def test_index_same():
    remove_index()
    client1 = get_client()
    index_path = os.path.join(get_dicom_source_dir(), INDEX_FILENAME)
    assert os.path.exists(index_path)
    index_mtime = os.stat(index_path).st_mtime

    client2 = get_client()
    assert client1.index.filepaths == client2.index.filepaths
    assert os.stat(index_path).st_mtime == index_mtime


def test_index_different():
    remove_index()
    client1 = get_client()
    index_path = os.path.join(get_dicom_source_dir(), INDEX_FILENAME)
    assert os.path.exists(index_path)
    index_size = os.stat(index_path).st_size

    new_ds = get_new_dataset()
    new_ds_path = os.path.join(get_dicom_source_dir(), 'new.dcm')
    dcmwrite(new_ds_path, new_ds, write_like_original=False)

    try:
        client2 = get_client()
        assert (len(client2.index.filepaths) - len(client1.index.filepaths)) == 1
        assert os.stat(index_path).st_size > index_size
    finally:
        os.remove(new_ds_path)


def test_search_added_dataset():
    remove_index()
    client = get_client()
    results = client.search_patients('new')
    assert len(results) == 0
    client.send_datasets([get_new_dataset()])
    results = client.search_patients('new')
    assert len(results) == 1





