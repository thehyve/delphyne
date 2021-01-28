"""Sphinx configuration."""

import os
import sys

sys.path.insert(0, os.path.abspath('..'))

project = "delphyne"
author = "The Hyve"
copyright = f"2021, {author}"
extensions = [
    "sphinx.ext.autodoc",
    "sphinx.ext.napoleon",
    "numpydoc",
]

# html_theme = "nature"
