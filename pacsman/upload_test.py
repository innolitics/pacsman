import os
import argparse

from pacsman import PynetDicomClient
from pacsman import dicom_file_iterator

TEST_DATA_DIRECTORY = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'test_dicom_data')
LOCAL_PACS_URL = os.environ.get('LOCAL_PACS_URL', 'localhost')


def main(destination='both'):
    print(f'uploading test data from {TEST_DATA_DIRECTORY}')

    if destination == 'both' or destination == 'remote':
        print('Uploading files to remote destination')
        client = PynetDicomClient(client_ae='TEST', pacs_url=LOCAL_PACS_URL, pacs_port=4242,
                                  dicom_dir='.', remote_ae='ORTHANC')
        client.send_datasets(dicom_file_iterator(TEST_DATA_DIRECTORY))

    if destination == 'both' or destination == 'local':
        print('Uploading files to local destination')
        # "TEST" must be registered AE in the Horos listener
        client = PynetDicomClient(client_ae='TEST', pacs_url=LOCAL_PACS_URL, pacs_port=11112,
                                  dicom_dir='.', remote_ae='HOROS-LOCAL')
        client.send_datasets(dicom_file_iterator(TEST_DATA_DIRECTORY))


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('destination', choices=['both', 'remote', 'local'], default='both', nargs='?')
    args = parser.parse_args()
    main(destination=args.destination)
