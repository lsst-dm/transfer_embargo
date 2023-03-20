#!/bin/bash

# Shell script for running the args file
# you also need the lsst_distrib loaded to run this file
# in order to run this file:
# chmod u+x run_move_embargo_scratch.sh
# and then ./run_move_embargo_scratch.sh
#"-fromrepo" = '/repo/embargo'

setup lsst_distrib
#run this if you need to create scratch butler
#butler create scratch

fromrepo = '/repo/embargo'
torepo = '/home/j/jarugula/scratch'
instrument = 'LATISS'
days = 30
dtype = 'raw'

python move_embargo_args.py --help
python move_embargo_args.py -fromrepo "$fromrepo" -torepo "$torepo" -instrument "$instrument" -days "$days" -dtype "$dtype"
