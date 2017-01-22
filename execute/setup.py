#!/usr/bin/env python

#/***************************************************************************
# *   Copyright (C) 2017 Daniel Mueller (deso@posteo.net)                   *
# *                                                                         *
# *   This program is free software: you can redistribute it and/or modify  *
# *   it under the terms of the GNU General Public License as published by  *
# *   the Free Software Foundation, either version 3 of the License, or     *
# *   (at your option) any later version.                                   *
# *                                                                         *
# *   This program is distributed in the hope that it will be useful,       *
# *   but WITHOUT ANY WARRANTY; without even the implied warranty of        *
# *   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the         *
# *   GNU General Public License for more details.                          *
# *                                                                         *
# *   You should have received a copy of the GNU General Public License     *
# *   along with this program.  If not, see <http://www.gnu.org/licenses/>. *
# ***************************************************************************/

from os.path import (
  dirname,
  join,
)
from setuptools import (
  setup,
)


def _importMeta():
  """Import and return the packages's __meta__ module."""
  # We must not import the package by normal means because if we have
  # dependencies those might not be resolved at the time of execution of
  # this script.
  with open(join(dirname(__file__), "src", "deso", "execute", "__meta__.py")) as f:
    globals_ = {}
    locals_ = {}
    exec(f.read(), globals_, locals_)
    return locals_


def retrieveName():
  """Retrieve the package's name."""
  return _importMeta()["name"]()


def retrieveVersion():
  """Retrieve the packages's version."""
  return _importMeta()["version"]()


def retrieveDescription():
  """Retrieve a description of the package."""
  return _importMeta()["description"]()


def retrieveLongDescription():
  """Retrieve a long description of the package."""
  with open(join(dirname(__file__), "README.rst")) as f:
    return f.read()


setup(
  name = retrieveName(),
  author = "Daniel Mueller",
  author_email = "deso@posteo.net",
  maintainer = "Daniel Mueller",
  maintainer_email = "deso@posteo.net",
  version = retrieveVersion(),
  description = retrieveDescription(),
  long_description = retrieveLongDescription(),
  url = "https://github.com/d-e-s-o/execute",
  classifiers = [
    "Development Status :: 4 - Beta",
    "Intended Audience :: Developers",
    "License :: OSI Approved :: GNU General Public License v3 (GPLv3)",
    "Operating System :: POSIX",
    "Programming Language :: Python",
    "Programming Language :: Python :: 3",
    "Topic :: Software Development :: Libraries :: Python Modules",
  ],
  keywords = "execute fork exec pipeline spring process development",
  license = "GPLv3",
  namespace_packages = [
    "deso",
  ],
  packages = [
    "deso.execute",
    "deso.execute.test",
  ],
  package_dir = {
    "deso.execute": join("src", "deso", "execute"),
    "deso.execute.test": join("src", "deso", "execute", "test"),
  },
  test_suite = "deso.execute.test.allTests",
  install_requires = [
    "cleanupd",
  ],
  zip_safe = False,
)
