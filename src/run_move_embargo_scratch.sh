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

arg_fromrepo="/repo/embargo"
arg_torepo="/home/j/jarugula/scratch"
arg_instrument="LATISS"
arg_hours=1.0
arg_dtype="raw"
arg_coll="LATISS/raw/all"

#python move_embargo_args.py --help
python move_embargo_args.py -f $arg_fromrepo -t $arg_torepo --instrument $arg_instrument --embargohours $arg_hours --datasettype $arg_dtype --collections $arg_coll
