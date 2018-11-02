class InvalidDicomError(Exception):
    '''
    This exception is raised when invalid DICOM data is present and can not be
    gracefully handled otherwise.
    '''
    pass
