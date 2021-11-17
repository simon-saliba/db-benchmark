#!/bin/bash
set -e

echo 'upgrading terality...'

source ./terality/py-terality/bin/activate

echo "skipping upgrade, because I manually edited the client lib"
#python -m pip install --upgrade terality > /dev/null
