#!/bin/bash

# Shell script for running the args file
# you also need the lsst_distrib loaded to run this file
# in order to run this file:
# chmod u+x run_move_embargo_scratch.sh
# and then ./run_move_embargo_scratch.sh

setup lsst_distrib
python move_embargo_args.py --help
