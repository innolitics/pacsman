import numpy as np
from pydicom import Dataset

from .utils import _scale_and_window_pixel_array_to_uint8, _pad_pixel_array_to_square, \
    copy_dicom_attributes, dicom_filename


def test_scale_pixel_array_to_png():
    arr = np.array([[0.5, 0.5], [1., 1.]], dtype=float)
    scaled = _scale_and_window_pixel_array_to_uint8(arr, 0.5, 1, 1, 0)
    assert np.array_equal(scaled, np.array([[0, 0], [255, 255]]))


def test_pad_png_pixel_array_already_square():
    arr = np.array([[1, 1, 1], [2, 2, 2], [3, 3, 3]])
    padded = _pad_pixel_array_to_square(arr)
    assert np.array_equal(arr, padded)


def test_pad_png_pixel_array_pad_right():
    arr = np.array([[1, 1], [2, 2], [3, 3]])
    padded = _pad_pixel_array_to_square(arr)
    expected = np.array([[1, 1, 255], [2, 2, 255], [3, 3, 255]])
    assert np.array_equal(padded, expected)


def test_pad_png_pixel_array_pad_down():
    arr = np.array([[1, 1, 1], [2, 2, 2]])
    padded = _pad_pixel_array_to_square(arr)
    expected = np.array([[1, 1, 1], [2, 2, 2], [255, 255, 255]])
    assert np.array_equal(padded, expected)


def test_copy_dicom_attributes():
    source_dataset = Dataset()
    destination_dataset = Dataset()
    destination_dataset.PatientName = 'Fred'
    additional_tags = ['PatientName']
    copy_dicom_attributes(destination_dataset, source_dataset, additional_tags)
    assert destination_dataset.PatientName == 'Fred'


def test_datasets_native_getattr_works():
    '''
    If this test fails, then that means you are using an older version of
    pydicom that has a bug causing `getattr` to ignore default values.
    '''
    assert getattr(Dataset(), 'PatientName', None) is None


def test_dicom_filename():
    dataset = Dataset()
    dataset.SOPInstanceUID = 'abracadabra'
    assert 'abracadabra.dcm' == dicom_filename(dataset)
