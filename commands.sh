#!/bin/bash
bash -c source /opt/lsst/software/stack/loadLSST.bash
setup lsst_obs
python src/move_embargo_args.py $FROMREPO $TOREPO $INSTRUMENT --log $LOG --pastembargohours $PASTEMBARGO $DATAQUERIES $OTHER_ARGUMENTS