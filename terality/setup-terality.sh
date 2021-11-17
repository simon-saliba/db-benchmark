#!/bin/bash
set -e

# install all dependencies
#sudo apt-get update
#sudo apt-get install build-essential python3-dev python3-pip

python3 -m virtualenv pandas/py-pandas --python=/usr/bin/python3
source pandas/py-pandas/bin/activate

# install binaries
python -m pip install --upgrade psutil
python -m pip install --upgrade terality
# some old versions of terality lack the explicit boto3 dependency
python -m pip install boto3

# install datatable for fast data import
python -m pip install --upgrade datatable

# check
python
import terality as pd
pd.__version__
quit()
deactivate
