# -*- coding: utf-8 -*-
import re
from os import path
from setuptools import find_packages, setup

ROOT_DIR = path.abspath(path.dirname(__file__))

DESCRIPTION = 'va_apiprovider'
LONG_DESCRIPTION = open(path.join(ROOT_DIR, 'README.rst')).read()
VERSION = re.search(
    "__version__ = '([^']+)'",
    open(path.join(ROOT_DIR, 'va_apiprovider', '__init__.py')).read()
).group(1)


setup(
    name='va_apiprovider',
    version=VERSION,
    description=DESCRIPTION,
    long_description=LONG_DESCRIPTION,
    url='https://github.com/sevencentco/va_apiprovider',
    author='VietAnh',
    author_email='sevencentco@gmail.com',
     classifiers=[
        "Programming Language :: Python :: 3",
        "Framework :: AsyncIO",
        "License :: OSI Approved :: License",
        "Operating System :: OS Independent",
    ],
    packages=find_packages(exclude=['tests*']),
    python_requires=">=3.8",
    include_package_data=True,
    install_requires=["sanic==25.3.0"],
    extras_require={},
    zip_safe=False,
    platforms='any',
   
)