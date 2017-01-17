#!/usr/bin/env bash

# Executes all the unit test, assuming all lib dependencies are available
# => this script is typically executed inside docker, or in your local
# environment if your have installed the required dependencies

export set PATH=$PATH:/home/ria/miniconda2/bin/
source activate workspace_py2.7

cd /home/ria/work
py.test -s --teamcity
#py.test -s tests/test_cdr.py
#py.test -s tests/test_snd.py
