import os
from typing import Iterable

import numpy as np
import png
import scipy.ndimage
from pydicom import Dataset, dcmread
from pydicom.multival import MultiValue
from pydicom.errors import InvalidDicomError


def process_and_write_png_from_file(thumbnail_dcm_path):
    '''
    :param thumbnail_dcm_path: DICOM instance file path. Must be unique per instance.
        The file is deleted to make the PNG.
    :return path to png (or None on failure). Uses the same name as the input file.
    '''
    if not os.path.exists(thumbnail_dcm_path):
        return None
    png_path = None
    try:
        thumbnail_ds = dcmread(thumbnail_dcm_path)
        png_path = os.path.splitext(thumbnail_dcm_path)[0] + '.png'
        process_and_write_png(thumbnail_ds, png_path)
    finally:
        os.remove(thumbnail_dcm_path)
    return png_path


def process_and_write_png(thumbnail_ds, png_path):
    '''
    :param thumbnail_ds: DICOM instance dataset with pixel array
    :param png_data: Output path for the thumbnail PNG

    Pads the instance pixel array with white to make it square, then scale to 100x100,
    and write out to png_path.
    '''
    thumbnail_slice = thumbnail_ds.pixel_array.astype(float)

    center_attr = dataset_attribute_fetcher(thumbnail_ds, 'WindowCenter')
    width_attr = dataset_attribute_fetcher(thumbnail_ds, 'WindowWidth')
    if center_attr and width_attr:
        center = center_attr[0] if isinstance(center_attr, MultiValue) else center_attr
        width = width_attr[0] if isinstance(width_attr, MultiValue) else width_attr
    else:
        # this is a CT soft tissue windowing in HU
        center = 40
        width = 400
    floor, roof = center - width / 2, center + width / 2,

    # RescaleSlope and RescaleIntercept have a defined VM of 1, but some PACS may not respect it
    slope_attr = getattr(thumbnail_ds, 'RescaleSlope', 1)
    slope = slope_attr[0] if isinstance(slope_attr, MultiValue) else slope_attr
    slope = float(slope)

    intercept_attr = getattr(thumbnail_ds, 'RescaleIntercept', 0)
    intercept = intercept_attr[0] if isinstance(intercept_attr, MultiValue) else intercept_attr
    intercept = float(intercept)

    png_scaled = _scale_and_window_pixel_array_to_uint8(thumbnail_slice, floor, roof,
                                                        slope, intercept)
    padded = _pad_pixel_array_to_square(png_scaled)

    # zoom to 100x100
    zoom_factor = 100 / padded.shape[0]
    png_array = scipy.ndimage.zoom(padded, zoom_factor, order=1)

    with open(png_path, 'wb') as f:
        writer = png.Writer(len(png_array[0]), len(png_array), greyscale=True)
        writer.write(f, png_array)


def _scale_and_window_pixel_array_to_uint8(arr, floor, roof, slope, intercept):
    '''
    Scales input float pixel array to 8 bit int for PNG writing.
    :param arr: stored value ndarray with type float
    :param floor: floor of window, values below are 0 / black
    :param roof: roof of window, values above are 255 / white
    :param slope: rescale slope to convert stored values to output units (e.g. HU for CT)
    :param intercept: rescale intercept to convert stored values to rescale type units
    :return: uint8 ndarray with same dimensions as input scaled between 0 and 255
    '''
    arr = arr * slope + intercept
    # png needs int values between 0 and 255
    result = np.zeros(arr.shape)
    result[arr >= roof] = 255
    inside_window = np.logical_and(arr > floor, arr < roof)
    result[inside_window] = (arr[inside_window] - floor) / (roof - floor) * 255
    return np.uint8(result)


def _pad_pixel_array_to_square(arr, pad_value=255):
    '''
    Pads the instance pixel array with value to make it square.
    Default is 255 (white for PNG)
    :param arr: Input scaled int ndarray
    :return: Square array padded with `pad_value`
    '''
    (a, b) = arr.shape
    if a > b:
        padding = ((0, 0), (0, a - b))
    else:
        padding = ((0, b - a), (0, 0))
    return np.pad(arr, padding, mode='constant', constant_values=pad_value)


def set_undefined_tags_to_blank(dataset, additional_tags):
    for tag in additional_tags or []:
        if not hasattr(dataset, tag) or getattr(dataset, tag) is None:
            setattr(dataset, tag, '')


def copy_dicom_attributes(destination, source, tags, missing='skip'):
    for tag in tags or []:
        if hasattr(source, tag):
            value = getattr(source, tag)
            setattr(destination, tag, value)
        elif missing == 'empty':
            setattr(destination, tag, '')
        elif missing != 'skip':
            raise ValueError(f'missing must be "skip" or "empty", not "{missing}"')


def dataset_attribute_fetcher(dataset, data_attribute):
    try:
        return getattr(dataset, data_attribute)
    except AttributeError:
        # Dataset has a bug where it ignores the default=None when getattr is called.
        return None


def dicom_file_iterator(folder: str) -> Iterable[Dataset]:
    for root, dirs, files in os.walk(folder):
        for file in files:
            dicom_file = os.path.join(root, file)
            try:
                dataset = dcmread(dicom_file)
                yield dataset
            except InvalidDicomError:
                pass


def dicom_filename(dataset: Dataset) -> str:
    return f'{dataset.SOPInstanceUID}.dcm'


def getattr_required(dataset, name):
    '''
    Helper function that should be used when accessing a required DICOM
    attribute, which should raise our standard exception upon a failure.
    '''
    try:
        return getattr(dataset, name)
    except AttributeError:
        raise InvalidDicomError(f"Missing required DICOM attribute {name}")
