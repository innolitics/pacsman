from datetime import date

import pytest
from pydicom import Dataset
from pydicom.uid import UID

from .dicom_interface import DicomInterface, PRIVATE_ID
from .exceptions import InvalidDicomError


def dataset_factory(defaults):
    '''
    Create a factory function for building pydicom datasets.

    If any values are functions, they will be called to determine the value of
    that attribute.  When the function is called, it will be passed an integer
    indicating the number of times the factory has been called, and the full
    attribute dict.

    If a value is `None`, then it is not set.

    :param defaults: Dict whose keys are tag names and values are tag values or functions.
    :return: A factory function that produces datasets.  Keyword arguments
        override the defaults.
    '''
    counter = 0

    def factory(**overrides):
        nonlocal counter
        ds = Dataset()
        attributes = {**defaults, **overrides}
        for key, value in attributes.items():
            if callable(value):
                value = value(counter, attributes)
            if value is not None:
                setattr(ds, key, value)
        counter += 1
        return ds
    return factory


@pytest.fixture
def slice_factory():
    defaults = {
        'PatientID': 'PAT001',
        'PatientName': 'John^Doe',
        'PatientBirthDate': date(2000, 1, 1),
        'StudyInstanceUID': '100000',
        'StudyDate': date(2018, 1, 1),
        'SOPInstanceUID': lambda i, a: f'1{i:05}',
    }
    return dataset_factory(defaults)


def test_build_patient_result_single_slice(slice_factory):
    slice_dataset = slice_factory()
    result = Dataset()
    DicomInterface.build_patient_result(result, slice_dataset)
    assert result.PatientID == slice_dataset.PatientID
    assert result.PatientName == slice_dataset.PatientName
    assert result.PacsmanPrivateIdentifier == PRIVATE_ID
    assert len(result.PatientStudyInstanceUIDs) == 1
    assert result.PatientStudyInstanceUIDs[0].name == slice_dataset.StudyInstanceUID.name
    assert result.PatientMostRecentStudyDate == slice_dataset.StudyDate


def test_build_patient_result_raise_if_id_change(slice_factory):
    slice_1 = slice_factory(PatientID='1')
    slice_2 = slice_factory(PatientID='2')
    result = Dataset()
    DicomInterface.build_patient_result(result, slice_1)
    with pytest.raises(ValueError):
        DicomInterface.build_patient_result(result, slice_2)


def test_build_patient_result_no_raise_if_name_change(slice_factory):
    '''
    At the moment, we grab values from the first dataset that we see, and we
    don't change any of them after that.  This is probably the desired
    behaviour, but it may be worth considering grabbing details from the most
    recent DICOM dataset
    '''
    result = Dataset()
    DicomInterface.build_patient_result(result, slice_factory(PatientName='1'))
    DicomInterface.build_patient_result(result, slice_factory(PatientName='2'))
    assert result.PatientName == '1'


def test_build_patient_result_multiple_studys(slice_factory):
    result = Dataset()
    DicomInterface.build_patient_result(result, slice_factory(StudyInstanceUID='1'))
    DicomInterface.build_patient_result(result, slice_factory(StudyInstanceUID='2'))
    assert len(result.PatientStudyInstanceUIDs) == 2
    assert {uid.name for uid in result.PatientStudyInstanceUIDs} == {'1', '2'}


def test_build_patient_result_single_study(slice_factory):
    result = Dataset()
    DicomInterface.build_patient_result(result, slice_factory(StudyInstanceUID='1'))
    DicomInterface.build_patient_result(result, slice_factory(StudyInstanceUID='1'))
    assert len(result.PatientStudyInstanceUIDs) == 1
    assert result.PatientStudyInstanceUIDs[0].name == '1'


def test_build_patient_result_most_recent_study_date(slice_factory):
    result = Dataset()
    DicomInterface.build_patient_result(result, slice_factory(StudyDate=date(2018, 1, 1)))
    assert result.PatientMostRecentStudyDate == date(2018, 1, 1)
    DicomInterface.build_patient_result(result, slice_factory(StudyDate=date(2018, 1, 2)))
    assert result.PatientMostRecentStudyDate == date(2018, 1, 2)
    DicomInterface.build_patient_result(result, slice_factory(StudyDate=date(2018, 1, 1)))
    assert result.PatientMostRecentStudyDate == date(2018, 1, 2)


def test_build_patient_result_missing_study_date(slice_factory):
    result = Dataset()
    DicomInterface.build_patient_result(result, slice_factory(StudyDate=''))
    assert result.PatientMostRecentStudyDate == ''
    DicomInterface.build_patient_result(result, slice_factory(StudyDate=date(2018, 1, 1)))
    assert result.PatientMostRecentStudyDate == date(2018, 1, 1)


@pytest.mark.parametrize('attribute', [
    'PatientID',
    'StudyInstanceUID',
])
def test_build_patient_result_unhandled_missing_tags(slice_factory, attribute):
    overrides = {attribute: None}
    with pytest.raises(InvalidDicomError):
        DicomInterface.build_patient_result(Dataset(), slice_factory(**overrides))


@pytest.mark.parametrize('attribute', [
    'PatientName',
    'PatientBirthDate',
    'StudyDate',
])
def test_build_patient_result_handled_missing_tags(slice_factory, attribute):
    overrides = {attribute: None}
    DicomInterface.build_patient_result(Dataset(), slice_factory(**overrides))


@pytest.mark.parametrize('attribute', [
    'PatientName',
    'PatientBirthDate',
    'StudyDate',
])
def test_build_patient_result_empty_tags(slice_factory, attribute):
    overrides = {attribute: ''}
    DicomInterface.build_patient_result(Dataset(), slice_factory(**overrides))
