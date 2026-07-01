"""
Setup configuration for the Hadoobernetes CLI.

Running `pip install -e .` in the cli/ directory will read this file
and install the CLI into your virtual environment, exposing the `hadoob` 
command globally.
"""

from setuptools import setup, find_packages

setup(
    name="hadoob",
    version="0.1.0",
    py_modules=["main", "auth", "storage", "api_client"],
    install_requires=[
        "click",
        "requests",
        "minio",
        "pydantic"
    ],
    entry_points={
        "console_scripts": [
            "hadoob = main:cli",
        ],
    },
)