import numpy
import png
import scipy.ndimage


def process_and_write_png(thumbnail_ds, png_path):
    '''
    :param thumbnail_ds: DICOM instance dataset with pixel array
    :param png_data: Output path for the thumbnail PNG

    Pads the instance pixel array with white to make it square, then scale to 100x100,
    and write out to png_path.
    '''
    thumbnail_slice = thumbnail_ds.pixel_array.astype(float)

    png_scaled = _scale_pixel_array_to_uint8(thumbnail_slice)

    padded = _pad_pixel_array_to_square(png_scaled)

    # zoom to 100x100
    zoom_factor = 100 / max(padded.shape[0], padded.shape[1])
    png_array = scipy.ndimage.zoom(padded, zoom_factor, order=1)

    with open(png_path, 'wb') as f:
        writer = png.Writer(len(png_array[0]), len(png_array), greyscale=True)
        writer.write(f, png_array)


def _scale_pixel_array_to_uint8(arr):
    '''
    Scales input float pixel array to 8 bit int for PNG writing.
    :param arr: ndarray with type float
    :return: uint8 ndarray with same dimensions as input scaled between 0 and 255
    '''
    # png needs int values between 0 and 255
    input_min = numpy.amin(arr)
    input_max = numpy.amax(arr)
    rescaled = (arr - input_min) * 255 / (input_max - input_min)
    return numpy.uint8(rescaled)


def _pad_pixel_array_to_square(arr, pad_value=255):
    '''
    Pads the instance pixel array with value to make it square.
    Default is 255 (white for PNG)
    :param arr: Input scaled int ndarray
    :return: Square array padded with `pad_value`
    '''
    (a, b) = arr.shape
    if a > b:
        padding = ((0, 0), (0, a-b))
    else:
        padding = ((0, b-a), (0, 0))
    return numpy.pad(arr, padding, mode='constant', constant_values=pad_value)
