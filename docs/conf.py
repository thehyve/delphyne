"""Sphinx configuration."""

import os
import sys

# Add project root to syspath
sys.path.insert(0, os.path.abspath('..'))

project = "delphyne"
author = "The Hyve"
copyright = f"2021, {author}"

extensions = [
    "sphinx.ext.autodoc",
    "sphinx.ext.napoleon",
    "sphinx.ext.autosectionlabel",
    # "numpydoc",
]

# autodoc settings
autodoc_default_options = {
    'members':          True,
    'undoc-members':    True,
}

add_module_names = False
autosummary_generate = True
autodoc_typehints = 'description'

# html_theme = "sphinx_rtd_theme"
# html_theme = "nature"
