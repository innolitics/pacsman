'''
This filesystem client can be used for prototyping and testing in development when a PACS
is not available. It may be slow if many datasets are present, particularly on startup, as
indexing requires loading every DICOM dataset in the `test_dicom_data` dir.

An index file, `.pacsman_index` is written to the `dicom_source_dir`. Any additions,
removals, or edits that change the size or filesystem modification time will trigger
a reindex.

Example data located in `test_dicom_data` dir:
 (from www.dicomserver.co.uk).

Patient ID PAT001 "Joe Bloggss" dob 19450703
    Study ID 1.2.826.0.1.3680043.11.1011
        Study Date 20180522
        CT Modality
            Series ID 1.2.826.0.1.3680043.6.86796.74495.20180522152336.14136.1.23
                1 Image

Patient ID PAT014 "Erica Richardson" dob 19520314
    Study ID 1.2.826.0.1.3680043.11.118
        Study Date 20180518
        CT Modality
            Series ID 1.2.826.0.1.3680043.6.51581.36765.20180518132103.25992.1.21
                5 images
    Study ID 1.2.826.0.1.3680043.11.118.1
'''
import glob
import os
import pickle
import shutil
import hashlib
from collections import defaultdict, namedtuple
from typing import List, Optional, Dict, Iterable, NamedTuple

from pydicom import dcmread, Dataset
from pydicom.valuerep import MultiValue
from pydicom.uid import UID

from .base_client import BaseDicomClient, PRIVATE_ID
from .utils import process_and_write_png, copy_dicom_attributes, dicom_filename


INDEX_FILENAME = ".pacsman_index"


def _default_empty_list():
    """
    This replaces `lambda: []` for defaultdicts, as pickle does not serialize lambdas.
    """
    return []


class FilesystemDicomIndex():
    def __init__(self):
        self.dicom_source_dir_hash: str = ''
        self.filepaths: List[str] = []
        self.patient_id_to_filepaths: Dict[str, List[int]] = \
            defaultdict(_default_empty_list)
        self.series_id_to_filepaths: Dict[str, List[int]] = \
            defaultdict(_default_empty_list)
        self.study_id_to_filepaths: Dict[str, List[int]] = \
            defaultdict(_default_empty_list)
        self.patient_name_to_filepaths: Dict[str, List[int]] = \
            defaultdict(_default_empty_list)


class FilesystemDicomClient(BaseDicomClient):
    def __init__(self, dicom_dir: str, dicom_source_dir: str, *args, **kwargs) -> None:
        """
        :param dicom_src_dir: source directory for *.dcm files
        :param dicom_dir: the DICOM output dir for image retrievals (same as other clients)
        """
        self.dicom_dir = dicom_dir
        os.makedirs(self.dicom_dir, exist_ok=True)
        self.dicom_source_dir = dicom_source_dir

        self.cached_dicom_datasets: Dict[str, Dataset] = {}

        # load and use the index if it is present and its hash matches the current dir
        index_path = self._filepath(INDEX_FILENAME)
        if os.path.exists(index_path):
            with open(index_path, 'rb') as f:
                self.index = pickle.load(f)
                if self.index.dicom_source_dir_hash != self._dicom_source_dir_hash():
                    self._reindex_source_dir()
        else:
            self._reindex_source_dir()

    def _filepath(self, filename: str) -> str:
        return os.path.join(self.dicom_source_dir, filename)

    def _dicom_source_dir_hash(self):
        """
        Build a hash of DICOM files in the `dicom_source_dir` using their names, sizes,
        and modification times.
        """
        h = hashlib.md5()
        for dicom_file in glob.glob(f'{self.dicom_source_dir}/**/*.dcm', recursive=True):
            h.update(dicom_file.encode())
            stat = os.stat(self._filepath(dicom_file))
            h.update(str(stat.st_size).encode())
            h.update(str(stat.st_mtime).encode())
        return h.hexdigest()

    def _reindex_source_dir(self) -> None:
        """
        Build an index for the `dicom_source_dir` and also write it out to .pacsman_index
        """
        self.index = FilesystemDicomIndex()
        for dicom_file in glob.glob(f'{self.dicom_source_dir}/**/*.dcm', recursive=True):
            filepath = self._filepath(dicom_file)
            dataset = self._read_dataset(filepath)
            self._index_dataset(dataset, filepath)
            self.cached_dicom_datasets[filepath] = dataset

        self.index.dicom_source_dir_hash = self._dicom_source_dir_hash()

        index_path = self._filepath(INDEX_FILENAME)
        with open(index_path, 'wb') as f:
            pickle.dump(self.index, f)

    def _read_dataset(self, filepath: str) -> Dataset:
        """
        `stop_before_pixels` is used to reduce mem usage & to a lesser extent, load time.
        For this filesystem client, any fetch operations are performed by simply
          copying the *.dcm files.
        :param filepath: a DICOM file path
        :return: a pydicom Dataset, without pixel data
        """
        return dcmread(filepath, stop_before_pixels=True)

    def _index_dataset(self, dataset: Dataset, filepath: str) -> Dataset:
        self.index.filepaths.append(filepath)
        path_idx = len(self.index.filepaths) - 1
        self.index.patient_id_to_filepaths[dataset.PatientID].append(path_idx)
        self.index.study_id_to_filepaths[dataset.StudyInstanceUID].append(path_idx)
        self.index.series_id_to_filepaths[dataset.SeriesInstanceUID].append(path_idx)
        self.index.patient_name_to_filepaths[dataset.PatientID].append(path_idx)

    def _get_dataset(self, filepath_index) -> Dataset:
        filepath = self.index.filepaths[filepath_index]
        if filepath in self.cached_dicom_datasets:
            return self.cached_dicom_datasets[filepath]
        else:
            # dataset is present in the index but not yet read
            dataset = self._read_dataset(filepath)
            self.cached_dicom_datasets[filepath] = dataset
            return dataset

    def verify(self) -> bool:
        return True

    def send_datasets(self, datasets: Iterable[Dataset]) -> None:
        """
        Send a dicom dataset, storing it transiently without writing to disk.
        :param datasets:
        :return:
        """
        for dataset in datasets:
            # The index requires a unique filepath, but the dataset isn't written to disk,
            #  so the instance ID is used instead.
            dummy_filename = str(dataset.SOPInstanceUID)
            self._index_dataset(dataset, dummy_filename)
            self.cached_dicom_datasets[dummy_filename] = dataset

    def search_patients(self, search_query: str, additional_tags: List[str] = None) -> List[Dataset]:
        patient_id_to_results = defaultdict(Dataset)
        # support limited * wildcard with "in string" test for each dataset
        search_query = search_query.replace('*', '')

        # Build patient-level datasets from the instance-level test data
        search_query = search_query.lower()
        found_indices = []
        for patient_name in self.index.patient_name_to_filepaths.keys():
            if search_query in patient_name.lower():
                indices = self.index.patient_name_to_filepaths[patient_name]
                found_indices.extend(indices)
        for patient_id in self.index.patient_id_to_filepaths.keys():
            if search_query in patient_id.lower():
                indices = self.index.patient_id_to_filepaths[patient_id]
                found_indices.extend(indices)

        for filepath_index in found_indices:
            dataset = self._get_dataset(filepath_index)
            result = patient_id_to_results[dataset.PatientID]
            self.update_patient_result(result, dataset, additional_tags)

        return list(patient_id_to_results.values())

    def search_series(self, query_dataset, additional_tags=None) -> List[Dataset]:
        # Build series-level datasets from the instance-level test data
        additional_tags = additional_tags or []
        result_datasets = []
        series_id = query_dataset.SeriesInstanceUID
        datasets = [self._get_dataset(fp) for fp in
                    self.index.series_id_to_filepaths[series_id]]
        for dataset in datasets:
            ds = Dataset()
            additional_tags += [
                'PatientName',
                'PatientBirthDate',
                'BodyPartExamined',
                'SeriesDescription',
                'PatientPosition',
            ]
            ds.PatientStudyInstanceUIDs = MultiValue(UID, [dataset.StudyInstanceUID])
            ds.PacsmanPrivateIdentifier = PRIVATE_ID
            ds.PatientMostRecentStudyDate = getattr(dataset, 'StudyDate', '')
            copy_dicom_attributes(ds, dataset, additional_tags)
            result_datasets.append(ds)
        return result_datasets

    def studies_for_patient(self, patient_id, additional_tags=None) -> List[Dataset]:
        # additional tags are ignored here; only tags available are already in the files
        study_id_to_dataset: Dict[str, Dataset] = {}

        # Return one dataset per study
        datasets = [self._get_dataset(fp) for fp
                    in self.index.patient_id_to_filepaths[patient_id]]
        for dataset in datasets:
            if dataset.StudyInstanceUID not in study_id_to_dataset:
                study_id_to_dataset[dataset.StudyInstanceUID] = dataset
        return list(study_id_to_dataset.values())

    def series_for_study(self, study_id, modality_filter=None, additional_tags=None) -> List[Dataset]:
        # Build series-level datasets from the instance-level test data
        series_id_to_dataset: Dict[str, Dataset] = {}
        datasets = [self._get_dataset(fp) for fp
                    in self.index.study_id_to_filepaths[study_id]]
        for dataset in datasets:
            modality_matches = modality_filter is None or getattr(dataset, 'Modality', '') in modality_filter
            if modality_matches:
                dataset.PacsmanPrivateIdentifier = PRIVATE_ID
                dataset.BodyPartExamined = getattr(dataset, 'BodyPartExamined', '')
                dataset.SeriesDescription = getattr(dataset, 'SeriesDescription', '')
                dataset.PatientPosition = getattr(dataset, 'PatientPosition', '')
                series_id = dataset.SeriesInstanceUID
                if series_id in series_id_to_dataset:
                    series_id_to_dataset[series_id].NumberOfSeriesRelatedInstances += 1
                else:
                    dataset.NumberOfSeriesRelatedInstances = 1
                    series_id_to_dataset[series_id] = dataset

        return list(series_id_to_dataset.values())

    def images_for_series(self, series_id, additional_tags=None, max_count=None) -> List[Dataset]:
        image_datasets = []
        datasets = [self._get_dataset(fp) for fp
                    in self.index.series_id_to_filepaths[series_id]]
        for dataset in datasets:
            series_matches = dataset.SeriesInstanceUID == series_id
            if series_matches:
                image_datasets.append(dataset)
            if max_count and len(image_datasets) >= max_count:
                break
        return image_datasets

    def fetch_images_as_dicom_files(self, series_id: str) -> Optional[str]:
        result_dir = os.path.join(self.dicom_dir, series_id)
        os.makedirs(result_dir, exist_ok=True)

        image_paths = [self.index.filepaths[i] for i in
                       self.index.series_id_to_filepaths[series_id]]
        found = len(image_paths) > 0

        for path in image_paths:
            shutil.copy(path, os.path.join(result_dir))

        if found:
            return result_dir
        else:
            return None

    def fetch_image_as_dicom_file(self, series_id: str, sop_instance_id: str) -> Optional[str]:
        result_dir = os.path.join(self.dicom_dir, series_id)
        os.makedirs(result_dir, exist_ok=True)

        series_items = {fp: self._get_dataset(fp) for fp
                           in self.index.series_id_to_filepaths[series_id]}

        for (path, ds) in series_items:
            if ds.SOPInstanceUID == sop_instance_id:
                return shutil.copy(path, os.path.join(result_dir))
        return None

    def fetch_thumbnail(self, series_id: str) -> Optional[str]:
        series_items = [(self.index.filepaths[i], self._get_dataset(i)) for i
                        in self.index.series_id_to_filepaths[series_id]]
        if not series_items:
            return None

        series_items = sorted(series_items, key=lambda t: t[1].SOPInstanceUID)

        thumbnail_series_path = series_items[len(series_items) // 2][0]
        shutil.copy(thumbnail_series_path, self.dicom_dir)

        thumbnail_filename = os.path.basename(thumbnail_series_path)
        dcm_path = os.path.join(self.dicom_dir, thumbnail_filename)
        try:
            thumbnail_ds = dcmread(dcm_path)
            png_path = os.path.splitext(dcm_path)[0] + '.png'
            process_and_write_png(thumbnail_ds, png_path)
        finally:
            os.remove(dcm_path)
        return png_path
