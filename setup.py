#!/usr/bin/env python

from setuptools import setup, find_packages  # type: ignore

with open("README.md") as readme_file:
    readme = readme_file.read()

setup(
    author="The Hyve",
    author_email="office@thehyve.nl",
    python_requires=">=3.6",
    classifiers=[
        "Development Status :: 2 - Pre-Alpha",
        "License :: OSI Approved :: GNU General Public License v3 (GPLv3)",
        "Natural Language :: English",
        "Programming Language :: Python :: 3",
    ],
    description="Wrapper for OMOP ETL projects",
    entry_points={"console_scripts": ["omop_etl_wrapper=omop_etl_wrapper.cli:main",],},
    install_requires=[],
    license="GNU General Public License v3",
    long_description=readme,
    long_description_content_type="text/markdown",
    package_data={"omop_etl_wrapper": ["py.typed"]},
    include_package_data=True,
    keywords="omop_etl_wrapper",
    name="omop_etl_wrapper",
    package_dir={"": "src"},
    packages=find_packages(include=["src/omop_etl_wrapper", "src/omop_etl_wrapper.*"]),
    setup_requires=[],
    url="https://github.com/thehyve/omop_etl_wrapper",
    version="0.1.0",
    zip_safe=False,
)
