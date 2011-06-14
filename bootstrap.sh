#!/bin/bash

test -e virtualenv.py || wget -q https://raw.github.com/pypa/virtualenv/1.6.1/virtualenv.py
python virtualenv.py --no-site-packages env
env/bin/pip install -r requirements.txt
