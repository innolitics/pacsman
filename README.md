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
