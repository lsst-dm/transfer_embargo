#!/bin/bash

# Shell script for creating a fake butler that you can use
# for testing.
# you also need the lsst_distrib loaded to run this file
# in order to run this file:
# chmod u+x create_test_butlers.sh
# and then ./create_test_butlers.sh

setup lsst_distrib
#run this if you need to create scratch butler
butler create ../../tests/data/fake_from

butler create ../../tests/data/fake_to

