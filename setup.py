#!/usr/bin/env python

from setuptools import setup

setup(
    name="PyIotdb",
    description="Python interface to iotdb",
    author="lifengchuan",
    author_email="lifengchuan2008@gmail.com",
    license="Apache License, Version 2.0",
    packages=['pyiotdb'],
    classifiers=[
        "Intended Audience :: Developers",
        "License :: OSI Approved :: Apache Software License",
        "Operating System :: OS Independent",
        "Topic :: Database :: Front-Ends",
    ],
    install_requires=[
        'future',
        'python-dateutil',
        'apache-iotdb==0.12.2',
        'more-itertools==8.3.0'
    ],
    extras_require={
        'sqlalchemy': ['sqlalchemy>=1.3.17'],
    },
    tests_require=[
        'mock>=1.0.0',
        'pytest',
        'pytest-cov',
        'requests>=1.0.0',
        'sqlalchemy>=1.3.17',
    ],
    package_data={
        '': ['*.rst'],
    },
    entry_points={
        'sqlalchemy.dialects': [
            'iotdb = pyiotdb.sqlalchemy_iotdb:IoTDBDialect',
        ],
    }
)
