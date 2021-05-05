import os
from distutils.core import setup

from setuptools import find_packages

current_directory = os.path.dirname(os.path.abspath(__file__))
try:
    with open(os.path.join(current_directory, 'README.md'), encoding='utf-8') as f:
        long_description = f.read()
except Exception:
    long_description = ''

setup(
    name='airtable_fdw',
    packages=find_packages('.'),
    version='1.0.0',
    license='MIT',
    description='Airtable Multicorn FDW for Postgres',
    long_description=long_description,
    long_description_content_type='text/markdown',
    author='Sebastian Szymbor',
    author_email='thesebas@users.noreply.github.com',
    url='',
    download_url='',
    keywords=[],
    install_requires=[
        'multicorn==1.4.0',
        'airtable-python-wrapper==0.15.2',
    ],
    classifiers=[
        "Development Status :: 3 - Alpha",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3"
    ]
)
