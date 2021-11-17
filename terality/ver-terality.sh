#!/bin/bash
set -e

source ./terality/py-terality/bin/activate
#python -c 'import terality as pd; open("terality/VERSION","w").write(pd.__version__); open("terality/REVISION","w").write(pd.__git_version__);' > /dev/null
python -c 'import terality as pd; open("terality/VERSION","w").write("0.8.9"); open("terality/REVISION","w").write("0.8.9");' > /dev/null
