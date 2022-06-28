from .dcmtk_client import _check_dcmtk_message_for_error


def test_stdout_error_checking():
    # Standard message, with error
    test_string = '''
D: ======================= END DIMSE MESSAGE =======================
I: Request Identifiers:
I: 
I: # Dicom-Data-Set
I: # Used TransferSyntax: Little Endian Implicit
I: (0008,0020) DA (no value available)                     #   0, 0 StudyDate
I: (0008,0052) CS [STUDY ]                                 #   6, 1 QueryRetrieveLevel
I: (0010,0010) PN (no value available)                     #   0, 0 PatientName
I: (0010,0020) LO [snipped]                                #  10, 1 PatientID
I: (0010,0030) DA (no value available)                     #   0, 0 PatientBirthDate
I: (0010,0040) CS (no value available)                     #   0, 0 PatientSex
I: (0020,000d) UI (no value available)                     #   0, 0 StudyInstanceUID
I: 
E: Find Failed, file: /tmp/tmp5ld6qp8a/find_input.dcm:
E: 0006:0207 DIMSE No data available (timeout in non-blocking mode)
E: Find SCU Failed: 0006:0207 DIMSE No data available (timeout in non-blocking mode)
I: Aborting Association''' # noqa
    assert(_check_dcmtk_message_for_error(test_string) == (0x0006, 0x0207))

    # Another error message
    test_string = '''
F: Association Request Failed: 0006:031b Failed to establish association
F: 0006:0317 Peer aborted Association (or never connected)
F: 0006:031c TCP Initialization Error: Connection refused'''
    assert(_check_dcmtk_message_for_error(test_string) == (0x006, 0x031c))

    # echoscu, no errors, verbose
    test_string = '''
I: Requesting Association
I: Association Accepted (Max Send PDV: 16372)
I: Sending Echo Request (MsgID 1)
I: Received Echo Response (Success)
I: Releasing Association
    '''
    assert(_check_dcmtk_message_for_error(test_string) is None)

    # Successful findscu, empty, verbose
    test_string = '''
I: Requesting Association
I: Association Accepted (Max Send PDV: 16372)
I: Sending Find Request (MsgID 1)
I: Request Identifiers:
I: 
I: # Dicom-Data-Set
I: # Used TransferSyntax: Little Endian Explicit
I: (0008,0052) CS [STUDY]                                  #   6, 1 QueryRetrieveLevel
I: (0008,103e) LO (no value available)                     #   0, 0 SeriesDescription
I: (0010,0010) PN [TEST]                                   #   4, 1 PatientName
I: 
I: Received Final Find Response (Success)
I: Releasing Association''' # noqa

    # Successful findscu, has results, non-verbose
    test_string = '''
I: ---------------------------
I: Find Response: 1 (Pending)
I: 
I: # Dicom-Data-Set
I: # Used TransferSyntax: Little Endian Explicit
I: (0008,0005) CS [ISO_IR 100]                             #  10, 1 SpecificCharacterSet
I: (0008,0052) CS [STUDY ]                                 #   6, 1 QueryRetrieveLevel
I: (0008,0054) AE [ORTHANC ]                               #   8, 1 RetrieveAETitle
I: (0008,103e) LO (no value available)                     #   0, 0 SeriesDescription
I: (0010,0010) PN [Lymphoma]                               #   8, 1 PatientName
I: 
I: ''' # noqa
    assert(_check_dcmtk_message_for_error(test_string) is None)

    # Handling an empty stdout
    assert(_check_dcmtk_message_for_error('') is None)
