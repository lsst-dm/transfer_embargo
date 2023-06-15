#!/bin/bash

# Shell script for creating a fake butler that you can use
# for testing.
# you also need the lsst_distrib loaded to run this file
# in order to run this file:
# chmod u+x create_testto_butler.sh
# and then ./create_testto_butler.sh

# setup lsst_distrib
#run this if you need to create scratch butler

butler create $1