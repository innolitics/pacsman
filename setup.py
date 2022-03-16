"""
A setuptools based setup module.
"""

from setuptools import setup, find_packages
from os import path

here = path.abspath(path.dirname(__file__))

description = 'pacsman: Picture Archiving and Communication System Manager And Numpifier'

setup(
    name='pacsman',
    version='0.1.0',
    description=description,
    long_description=description,
    url='https://github.com/innolitics/pacsman',
    author='Innolitics, LLC',
    author_email='info@innolitics.com',
    license='MIT',
    classifiers=[
        'Development Status :: 1 - Planning',

        'Intended Audience :: Developers',
        'Intended Audience :: Healthcare Industry',
        'Topic :: Scientific/Engineering :: Medical Science Apps.',
        'Topic :: Communications',

        'License :: OSI Approved :: MIT License',

        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.6',
    ],

    keywords='scientific image',

    packages=find_packages(exclude=['contrib', 'docs', 'tests']),

    install_requires=['pydicom>=1.3', 'pynetdicom>=2', 'numpy', 'pypng', 'scipy'],

    dependency_links=['git+https://github.com/pydicom/pynetdicom3.git#egg=pynetdicom3'],

    extras_require={},

    package_data={},
    data_files=[],
)
