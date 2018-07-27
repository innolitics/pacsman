import dicom_numpy
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
    # combine_slices needs more than one slice to check spacing; we just ignore the 2nd
    thumbnail_arr, _ = dicom_numpy.combine_slices([thumbnail_ds, thumbnail_ds])
    thumbnail_slice = thumbnail_arr[:, :, 0]

    thumbnail_slice = thumbnail_slice.astype(float)

    # png needs int values between 0 and 255
    input_min = numpy.amin(thumbnail_slice)
    input_max = numpy.amax(thumbnail_slice)
    png_scaled = (thumbnail_slice - input_min) * 255 / (input_max - input_min)

    (a, b) = png_scaled.shape
    if a > b:
        padding = ((0, 0), (0, a-b))
    else:
        padding = ((0, b-a), (0, 0))
    padded = numpy.pad(png_scaled, padding, mode='constant', constant_values=255)

    # zoom to 100x100
    zoom_factor = 100 / max(a, b)
    png_array = numpy.uint8(scipy.ndimage.zoom(padded, zoom_factor, order=1))

    with open(png_path, 'wb') as f:
        writer = png.Writer(len(png_array[0]), len(png_array),
                            greyscale=True)
        writer.write(f, png_array)
