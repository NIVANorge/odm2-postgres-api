# !/usr/bin/env python
# -*- coding: utf-8 -*-

import io
import os

from setuptools import find_packages, setup

# Package meta-data.
NAME = 'odm2-postgres-api'
DESCRIPTION = 'A lightweight and fast api for interacting with odm2'
URL = 'https://github.com/NIVANorge/odm2-postgres-api'
EMAIL = 'roald.storm@niva.no'
AUTHOR = 'Roald Storm'
REQUIRES_PYTHON = '>=3.7.0'
VERSION = None

# What packages are required for this module to be executed?
REQUIRED = [
    'asyncpg',
    'fastapi',
    'uvicorn'
]

# What packages are optional?
EXTRAS = {
    # 'fancy feature': ['some_pakcage'],
}

TEST_REQUIRES = [
    ['pytest',
     'requests']
]

here = os.path.abspath(os.path.dirname(__file__))

# Import the README and use it as the long-description.
# Note: this will only work if 'README.md' is present in your MANIFEST.in file!
try:
    with io.open(os.path.join(here, 'README.md'), encoding='utf-8') as f:
        long_description = '\n' + f.read()
except FileNotFoundError:
    long_description = DESCRIPTION

# Load the package's __version__.py module as a dictionary.
about = {}
if not VERSION:
    project_slug = NAME.lower().replace("-", "_").replace(" ", "_")
    with open(os.path.join(here, 'src', project_slug, '__version__.py')) as f:
        exec(f.read(), about)
else:
    about['__version__'] = VERSION

# Where the magic happens:
setup(
    name=NAME,
    version=about['__version__'],
    description=DESCRIPTION,
    long_description=long_description,
    long_description_content_type='text/markdown',
    author=AUTHOR,
    author_email=EMAIL,
    python_requires=REQUIRES_PYTHON,
    url=URL,
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    install_requires=REQUIRED,
    extras_require=EXTRAS,
    tests_require=TEST_REQUIRES,
    include_package_data=True,
    license='Owned by NIVA',
    classifiers=[
        # Trove classifiers
        # Full list: https://pypi.python.org/pypi?%3Aaction=list_classifiers
        'Programming Language :: Python',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: Implementation :: PyPy'
    ]
)
