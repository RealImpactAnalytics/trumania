#!/usr/bin/env bash

# Executes all the unit test, assuming all lib dependencies are available
# => this script is typically executed inside docker (or anywhere the lib
# are available)

export set PATH=$PATH:/home/ria/miniconda2/bin/
source activate workspace_py2.7

cd /home/ria/work
pip install -e .
py.test -s