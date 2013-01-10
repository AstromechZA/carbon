#!/usr/bin/env python

import sys
from os.path import dirname, join, abspath

# Figure out where we're installed
BIN_DIR = dirname(abspath(__file__))
ROOT_DIR = dirname(BIN_DIR)

# Make sure that carbon's 'lib' dir is in the $PYTHONPATH if we're running from
# source.
LIB_DIR = join(ROOT_DIR, 'lib')
sys.path.insert(0, LIB_DIR)

from carbon.util import run_twistd_plugin

run_twistd_plugin(__file__)
