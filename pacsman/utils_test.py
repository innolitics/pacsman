import numpy as np
from pydicom import Dataset
import pytest

from .utils import _scale_pixel_array_to_uint8, _pad_pixel_array_to_square, copy_dicom_attributes, \
        getattr_dataset


def test_scale_pixel_array_to_png():
    arr = np.array([[0.5, 0.5], [1., 1.]], dtype=float)
    scaled = _scale_pixel_array_to_uint8(arr)
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


def test_datasets_native_getattr_fails():
    '''
    If this test fails, then that means pydicom has fixed the bug that made
    `utils.getattr_dataset` necessary.  Once this happens, we can remove this
    test, the tests for `utils.getattr_datset`, and replace
    `utils.getattr_dataset` with the native `getattr`.
    '''
    ds = Dataset()
    with pytest.raises(AttributeError):
        ds.getattr('PatientName', None)


def test_getattr_datasets_with_default():
    ds = Dataset()
    value = getattr_dataset(ds, 'PatientName', None)
    assert value is None


def test_getattr_datasets_no_default():
    ds = Dataset()
    with pytest.raises(AttributeError):
        getattr_dataset(ds, 'PatientName')
