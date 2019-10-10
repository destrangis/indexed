#! /usr/bin/env python3
from setuptools import setup

from indexed import VERSION

with open("README.md") as rdf:
    long_descr = rdf.read()

setup(
    name="indexed",
    version=VERSION,
    py_modules=["indexed"],
    author="Javier Llopis",
    author_email="javier@llopis.me",
    url="https://github.com/destrangis/indexed",
    description="Indexed files implementation",
    long_description_content_type="text/markdown",
    long_description=long_descr,
    classifiers = [
        "Development Status :: 5 - Production/Stable",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Programming Language :: Python",
        "Programming Language :: Python :: 3 :: Only",
        "Topic :: Software Development",
        "Topic :: Software Development :: Libraries",
        "Topic :: Software Development :: Libraries :: Python Modules",
        ]
)
