![example workflow](https://github.com/innolitics/pacsman/actions/workflows/unit-tests.yml/badge.svg)

# `pacsman`: Picture Archiving and Communication System Manager And Numpifier

`pacsman` is a utility to manage interactions with a PACS in Python. It has a
`BaseDicomClient` that provides abstract methods for a variety of DICOM-related
interactions, and it supplies several backends that implement these methods using tools
such as:

- DCMTK
- pynetdicom
- your local filesystem

It provides the ability to fetch images and render as thumbnail PNGs, or fetch raw DICOM
files.

In addition to the supplied backends, you can write your own backend implementing the
`BaseDicomClient`. This can be a useful interface layer for non-PACS systems such as a
cloud storage system.

## Development
Linting is done with `flake8` and testing with `pytest`.

GitHub actions has automatic checks for both linting and tests, using [`tox`](https://tox.wiki/en/latest/) as the runner (see [`./tox.ini`](tox.ini)). To replicate this locally, install `tox`, then run `tox .` in the root of the project.

> If you get an error about `DCMDICTPATH` or `SCPCFGPATH` not being found, change the `tox.ini` setenv values to your local path to the referenced files. 

> If you get an error about "InterpreterNotFound", make sure you have that version of Python installed and in the path (e.g., discoverable with `which python{version}`). Or use `--skip-missing-interpreters` to skip those.


### Remote DICOM Testing - Using Orthanc
Tests marked *remote* rely on a live DICOM server. For GitHub actions, an instance of Orthanc will be used, and you can re-use this service locally as well.

```bash
docker-compose up -d orthanc

# If this is the first time, and test data files have not yet been loaded
python3 pacsman/upload_test.py remote

# If you have DCMTK installed, here is a quick test
echoscu localhost 4242

# Or, with pynetdicom
python -m pynetdicom echoscu localhost 4242
```
