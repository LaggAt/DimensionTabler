#!/usr/bin/env python
# -*- coding: utf-8 -*-
# (c) 2018 Florian Lagg <github@florian.lagg.at>
# Under Terms of GPL v3

import os
from setuptools import setup

def read(fname):
    return open(os.path.join(os.path.dirname(__file__), fname)).read()

def get_long_description():
    readme_file = "README.md"
    if not os.path.isfile(readme_file):
        return ""
    # Try to transform the README from Markdown to reStructuredText.
    try:
        os.system("pandoc --from=markdown --to=rst --output=README.rst README.md")
        description = open("README.rst").read()
        os.remove("README.rst")
    except Exception:
        description = open(readme_file).read()
    return description

setup(
    name = "DimensionTabler",
    version = "0.1.2",
    author = "Florian Lagg",
    author_email = "github@florian.lagg.at",
    description = ("Builds dimension tables with configurable/variable grainularity."
        "Keeps those tables up-to-date. This makes reporting queries performant and doable. I use it with grafana."),
    license = "GPL v3",
    keywords = "dimension tables database grafana bi business intelligence",
    url = "https://github.com/LaggAt/DimensionTabler",
    packages=['DimensionTabler'], #, 'tests'],
    install_requires=[
        "more-itertools >= 4.1.0",
    ],
    long_description=get_long_description(),
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "Intended Audience :: Science/Research",
        "Programming Language :: Python :: 2.7",
        "Topic :: Utilities",
        "Topic :: Database",
        "Topic :: Office/Business :: Financial",
        "License :: OSI Approved :: GNU General Public License v3 (GPLv3)",
    ],
)