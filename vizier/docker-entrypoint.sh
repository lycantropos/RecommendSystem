#!/usr/bin/env bash
set -e

python3.6 -m pip install -r requirements.txt

python3.6 manage.py --verbose "$@"
