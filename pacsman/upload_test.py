import os

from pacsman.pacsman import PynetdicomClient
from pacsman.pacsman.utils import dicom_file_iterator

TEST_DATA_DIRECTORY = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'test_dicom_data')
LOCAL_PACS_URL = os.environ.get('LOCAL_PACS_URL', 'localhost')

def main():
    print(f'uploading test data from {TEST_DATA_DIRECTORY}')
    client = PynetdicomClient(client_ae='TEST', pacs_url=LOCAL_PACS_URL, pacs_port=11112,
                     dicom_dir='.')
    client.send_datasets(dicom_file_iterator(TEST_DATA_DIRECTORY))


if __name__ == '__main__':
    main()